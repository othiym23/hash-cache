'use strict'

var readFileSync = require('fs').readFileSync
  , createReadStream = require('fs').createReadStream
  , createWriteStream = require('fs').createWriteStream
  , join = require('path').join
  , inherits = require('util').inherits

var test = require('tap').test
  , mkdirp = require('mkdirp')
  , rimraf = require('rimraf')
  , sha = require('sha')

var Cache = require('../')

/*
 * CONSTANTS
 */

var PATH = join(__dirname, '/test-npm')
var OUTPUT = join(PATH, 'output', 'npm-e939de96.tgz')
var DIGEST = 'e939de969b38b3e7dfaa14fbcfe7a2fd15a4ea37'
var FIXTURE = join(__dirname, 'fixtures', 'readdir-scoped-modules-1.0.0.tgz')

// npm-like cache
function NPMCache(options) { Cache.call(this, options) }
inherits(NPMCache, Cache)

NPMCache.prototype._storePath = function (digest) {
  return join(this.path, 'pool', digest[0], digest[1], digest + '.tgz')
}

// comment this out to examine what the tests are doing
process.on('exit', function () { rimraf.sync(PATH) })

/*
 * TESTS
 */

test('input stream, with digest', function (t) {
  var input = createReadStream(FIXTURE)
  lifecycle(t, {stream : input, digest : DIGEST})
})

test('input stream, no digest', function (t) {
  var input = createReadStream(FIXTURE)
  lifecycle(t, {stream : input})
})

test('file input, with digest', function (t) {
  var input = FIXTURE
  lifecycle(t, {file : input, digest : DIGEST})
})

test('file input, no digest', function (t) {
  var input = FIXTURE
  lifecycle(t, {file : input})
})

test('content blob, with digest', function (t) {
  var input = readFileSync(FIXTURE)
  lifecycle(t, {contents : input, digest : DIGEST})
})

test('content blob, no digest', function (t) {
  var input = readFileSync(FIXTURE)
  lifecycle(t, {contents : input})
})

/**
 * test template
 */
function lifecycle(t, options) {
  rimraf(PATH, function (error) {
    t.ifError(error, 'cleaned up without issue')

    mkdirp.sync(join(PATH, 'output'))

    var cache = new NPMCache({
      path     : PATH,
      hash     : 'sha1',
      paranoid : true
    })

    cache.get(DIGEST, {type : 'stream'}, function (error) {
      t.equal(error.code, 'ENOENT', "package isn't already in cache")

      cache.put(options, function (error) {
        t.ifError(error, 'put package in cache')

        cache.get(DIGEST, {type : 'null'}, function (error) {
          t.ifError(error, 'package validated')

          cache.get(DIGEST, {type : 'stream'}, function (error, wad) {
            t.ifError(error, 'package is present and valid in cache')

            wad
              .pipe(sha.stream(DIGEST))
              .pipe(createWriteStream(OUTPUT))
              .on('finish', function () {
                sha.check(OUTPUT, DIGEST, function (error) {
                  t.ifError(error, 'fetched package matches digest')

                  cache.remove(DIGEST, function (error) {
                    t.ifError(error, 'removed package from cache')

                    cache.get(DIGEST, {type : 'stream'}, function (error) {
                      t.equal(error.code, 'ENOENT', 'package is no longer in cache')

                      t.end()
                    })
                  })
                })
              })
          })
        })
      })
    })
  })
}
