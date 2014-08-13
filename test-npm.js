'use strict'

var Cache = require('./')
  , createReadStream = require('fs').createReadStream
  , createWriteStream = require('fs').createWriteStream
  , assert = require('assert')
  , join = require('path').join
  , inherits = require('util').inherits
  , mkdirp = require('mkdirp')
  , rimraf = require('rimraf')
  , sha = require('sha')

function NPMCache(options) {
  Cache.call(this, options)
}
inherits(NPMCache, Cache)

NPMCache.prototype._storePath = function (digest) {
  return join(this.path, 'pool', digest[0], digest[1], digest + '.tgz')
}

var path = join(__dirname, '/test-npm')
// comment this out to examine what the tests are doing
process.on('exit', function () { rimraf.sync(path) })

rimraf(path, function (error) {
  assert(!error, 'cleaned up OK')

  mkdirp.sync(join(path, 'output'))

  var cache = new NPMCache({
    path     : path,
    hash     : 'sha1',
    paranoid : true
  })

  var input = createReadStream(join(
    __dirname, 'test', 'fixtures', 'readdir-scoped-modules-1.0.0.tgz'
  ))
  var output = join(path, 'output', 'npm-e939de96.tgz')
  var digest = 'e939de969b38b3e7dfaa14fbcfe7a2fd15a4ea37'

  cache.get(digest, {type : 'stream'}, function (error) {
    assert(error.code === 'ENOENT', "package isn't already in cache")

    cache.put({stream : input}, function (error, computed) {
      assert(!error, 'streaming package into cache did not error')
      assert(computed === digest, 'computed hash matches precalculated')

      cache.get(digest, {type : 'null'}, function (error) {
        assert(!error, 'package validated without issue')

        cache.get(digest, {type : 'stream'}, function (error, wad) {
          assert(!error, 'package is present and valid in cache')

          wad
            .pipe(sha.stream(digest))
            .pipe(createWriteStream(output))
            .on('finish', function () {
              sha.check(output, digest, function (error) {
                assert(!error, 'fetched package checks out')

                cache.remove(digest, function (error) {
                  assert(!error, 'no difficulty in removing package from cache')

                  cache.get(digest, {type : 'stream'}, function (error) {
                    assert(error.code === 'ENOENT', 'package is no longer in cache')

                    console.log('ok')
                  })
                })
              })
            })
        })
      })
    })
  })
})
