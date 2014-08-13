'use strict'

module.exports = Cache
var Path = require('path')
  , assert = require('assert')
  , fs = require('fs')
  , mkdirp = require('mkdirp')
  , through = require('through')
  , concat = require('concat')
  , once = require('once')
  , sha = require('sha')
  , RE_HEX = /^[0-9a-f]{32,}$/

function noop() {}
function unimplemented() { throw new Error('unimplemented') }

function Cache(opts) {
  if (!this || this === global) return new Cache(opts)

  if (typeof opts === 'string') opts = { path: opts }
  if (typeof opts.path !== 'string') throw new TypeError('path must be a string')

  this.path = opts.path + ''
  this.paranoid = !!opts.paranoid
  this.timeout = opts.timeout | 0
  this.__pending = Object.create(null)
  this.hash = opts.hash
}

Cache.prototype._storePath = function(digest) { return Path.join(this.path, 'store', digest) }
Cache.prototype._tmpPath = function(digest) { return Path.join(this.path, 'tmp', digest) }

Cache.prototype._createReadStream = unimplemented
Cache.prototype._createHash = unimplemented

Cache.prototype.createReadStream = function(digest) { var self = this
  digest = String(digest).toLowerCase()

  var output = through()

  if (!RE_HEX.test(digest)) {
    process.nextTick(function() {
      output.emit('error', new Error('not a valid hash: `' + digest + '`'))
    })
    return output
  }

  var pending = this.__pending[digest]
  if (!pending) {
    pending = through()

    var removed = false
      , remove = function() {
          if (removed) return
          pending.removeListener('error', remove)
          delete self.__pending[digest]
          removed = true
        }
    pending
      .once('readable', remove)
      .once('error', remove)

    pending.setMaxListeners(0)
    this.__pending[digest] = pending
    this.__acquire(arguments, pending)
  }

  return pending
    .on('error', function(err) { output.emit('error', err) })
    .pipe(output)
}

Cache.prototype.__acquire = function(args, pending, safe) { var self = this
  var digest = args[0]
    , error = errorFn(pending)

  var paranoidRead = this.paranoid && !safe
    , fd = typeof safe === 'number' ? safe : null

  var input = fs.createReadStream(
    this._storePath(digest),
    { autoClose: !paranoidRead, fd: fd, start: 0 }
  ).on('error', function(err) {
    if (err.code !== 'ENOENT') return error(err)

    self.__acquireFresh(args, pending)
  })

  if (!paranoidRead) return input.on('open', function() { this.pipe(pending) })

  input.on('open', function(fd) {
    error.cleanup.push(function() { fs.close(fd, noop) })
    self.__hash(input, digest, function(err) {
      if (err) return error(err)
      self.__acquire(args, pending, fd)
    })
  })
}

Cache.prototype.__acquireFresh = function(args, pending) { var self = this
  var digest = args[0]
    , store = this._storePath(digest)
    , tmp = this._tmpPath(digest)
    , error = errorFn(pending)

  mkdirp(Path.dirname(tmp), function(err) { if (err) { error(err) } else { writeStream()} })

  var output
  function writeStream() {
    output = fs.createWriteStream(tmp, { flags: 'wx' })
      .on('open', function(fd) {
        // it's ours! yay!
        // let's make sure we clean up, no matter what happens
        error.cleanup.push(function() { fs.unlink(tmp, noop) })
        readStream()

        // if we have a timeout, do our best to ensure it doesn't trigger
        if (!self.timeout) return
        var interval = setInterval(touch, self.timeout / 2)
        output.on('close', clearTouch)
        error.cleanup.push(clearTouch)

        function touch() {
          var present = +new Date()
          fs.futimes(fd, present, present, noop)
          setTimeout(touch, self.timeout / 2)
        }

        function clearTouch() {
          clearInterval(interval)
          interval = null
        }
      })
      .on('error', function(err) {
        if (err.code !== 'EEXIST') return error(err)
        // someone else has already started fetching this, we'll wait for them
        return self.__acquireWatch(args, pending)
      })
  }

  var input
  function readStream() {
    try { input = self._createReadStream.apply(self, args) }
    catch (e) { return error(e) }
    input.on('error', error)
    pipe()
  }

  function pipe() {
    self.__hash(input, digest, error.else(makeStore))
      .pipe(output)
  }

  function makeStore() {
    mkdirp(Path.dirname(store), error.else(rename))
  }

  function rename() {
    // if our tmpfile has been unlinked due to timeouts, we fail hard here.
    // even worse, if someone else has started writing, we put a partial file there.
    // that's unfortunate, but there's not a lot better we can do.
    fs.rename(tmp, store, error.else(deliver))
  }

  function deliver() {
    // up we go again
    // we explicitly disable paranoid mode, because we just checked the hash of what we wrote
    self.__acquire(args, pending, true)
  }
}

Cache.prototype.__acquireWatch = function(args, pending) { var self = this
  var digest = args[0]
    , tmp = self._tmpPath(digest)
    , error = errorFn(pending)

  // let's watch if they finish
  var watcher = fs.watch(tmp)
    .on('change', retry)
    .on('error', function(err) {
      if (err.code !== 'ENOENT') return error(err)
      retry()
    })

  error.cleanup.push(cleanup)
  function cleanup() {
    if (!watcher) return
    watcher.close()
    watcher = null
  }

  function retry() {
    if (!watcher) return
    cleanup()
    // they already finished while we were firing up our watcher. let's have another go at everything.
    self.__acquire(args, pending)
  }

  // if we have a timeout, let's check if things haven't gone stale
  // we'll leave this until the next 100ms so we don't fire this up too quickly, stat calls cost
  if (self.timeout) setTimeout(checkStale, 100)
  function checkStale() {
    fs.stat(tmp, function(err, stats) {
      if (!watcher) return null
      if (err && err.code === 'ENOENT') return retry()
      if (err) return error(err)

      var delta = new Date() - stats.mtime

      // not stale yet, we'll check again in a bit
      if (delta < self.timeout)
        return setTimeout(checkStale, self.timeout / 2)

      // stale file. goodbye!
      fs.unlink(tmp, function(err) {
        if (err && err.code === 'ENOENT') return null
        if (err) return error(err)
        retry()
      })
    })
  }
}

Cache.prototype.__hash = function(stream, digest, cb) {
  var hash = this._createHash()
  return stream
    .on('data', function(chunk) { hash.update(chunk) })
    .on('end', function() {
      var actualDigest = new Buffer(hash.digest()).toString('hex')
      if (actualDigest === digest) return cb()
      var err = new Error('hashes did not match. expected `' + digest + '`, got `' + actualDigest + '`')
      err.expected = digest
      err.actual = actualDigest
      cb(err)
    })
}

/**
 * type can be:
 *   "stream": a stream of the contents of the cached object (default)
 *   "contents": a Buffer containing the contents of the cached object
 *   "file": write the response to 'file' on disk, requires "path" option
 *   "null": discard the object and just validate the object
 */
Cache.prototype.get = function (digest, options, cb) {
  assert(cb, 'must pass callback in to get')
  assert(digest, 'must include the digest to fetch from the cache')

  var cleaned = String(digest).toLowerCase()
  assert(RE_HEX.test(cleaned), 'not a valid hash: `' + cleaned + '`')

  options = options || {}
  var responseType = options.type || 'stream'
  if (responseType === 'file') {
    assert(options.path, 'must include path when writing to file')
  }

  var cached = this._storePath(cleaned)
  var input, output
  switch (responseType) {
    case 'stream':
      input = fs.createReadStream(cached)
      input.on('error', cb)

      input.pipe(sha.stream(digest, {algorithm : this.hash}))
      input.on('open', function () { cb(null, input) })
      break

    case 'contents':
      fs.createReadStream(cached)
        .pipe(sha.stream(digest, {algorithm : this.hash}))
        .pipe(concat(cb))
      break

    case 'file':
      var onced = once(cb)
      input = fs.createReadStream(cached)
      input.on('error', onced)

      output = fs.createWriteStream(options.path)
      output.on('error', onced)

      input.pipe(output).on('finish', function() {
        onced(null, options.path)
      })
      break

    case 'null':
      fs.createReadStream(cached)
        .pipe(sha.stream(digest, {algorithm : this.hash}))
        .on('finish', function() {
           cb()
        })
      break

    default:
      throw new Error('unrecognized response type ' + responseType)
  }
}

Cache.prototype.put = function (options, cb) {
  assert(options, 'must include options to put')
  assert(
    options.stream || options.contents || options.file,
    'must include one of a stream, object contents, or file name'
  )
  assert(cb, 'must pass callback in to put')

  var self = this

  if (options.stream) {
    mkdirp(Path.join(this.path, 'tmp'), function(err) {
      if (err) return cb(err)

      var tmp = self._tmpPath(rand().toString(16))
      var output = fs.createWriteStream(tmp)
      output.on('error', cb)
      output.on('finish', function () {
        if (!options.digest) {
          return sha.get(tmp, {algorithm : self.hash}, function (err, digest) {
            if (err) {
              cb(err)
              return
            }

            self.put({file : tmp, digest : digest}, cb)
          })
        }

        self.put({file : tmp, digest : options.digest})
      })
      options.stream.pipe(output)
    })
  }
  else if (options.file) {
    if (!options.digest) {
      return sha.get(options.file, {algorithm : self.hash}, function (err, digest) {
        if (err) return cb(err)

        self.put({file : options.file, digest : digest}, cb)
      })
    }

    sha.check(options.file, options.digest, {algorithm : this.hash}, function (err) {
      if (err) return cb(err)

      var stored = self._storePath(options.digest)
      mkdirp(Path.dirname(stored), function (err) {
        if (err) return cb(err)

        fs.rename(options.file, stored, function (err) {
          if (err) return cb(err)

          cb(null, options.digest)
        })
      })
    })
  }

  function rand() {
    return (Math.random() * 0xFFFFFFFF) | 0
  }
}

Cache.prototype.remove = function (digest, cb) {
  var goner = this._storePath(digest)
  fs.unlink(goner, cb)
}

function errorFn(pending) {
  function error(err) {
    error.cleanup.forEach(function(fn) { fn() })
    pending.emit('error', err)
  }
  error.cleanup = []

  error.else = function(fn) {
    return function(err) {
      if (err) error(err)
      else fn()
    }
  }

  return error
}
