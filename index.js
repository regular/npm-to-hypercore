'use strict'

var ChangesStream = require('changes-stream')
var through2 = require('through2')
var pump = require('pump')
var path = require('path')
var Pushable = require('pull-pushable')

module.exports = function getStream(seq) {
  var clean = require('normalize-registry-metadata')

  var normalize = through2.obj(transform)
  var running = true 
  var pushable = Pushable(true, function(err) {
    running = false
  })
  
  run()
  return pushable.source

  function run () {
    console.log('feching changes since sequence #%s', seq)
    pump(changesSinceStream(seq), normalize, function(err) {
      pushable.end(err)
    })
  }

  function changesSinceStream (seq) {
    return new ChangesStream({
      db: 'https://replicate.npmjs.com',
      include_docs: true,
      since: seq,
      highWaterMark: 4 // reduce memory - default is 16
    })
  }

  function transform (change, env, cb) {
    var doc = change.doc

    clean(doc)

    var modified = doc && doc.time && doc.time.modified
    var seq = change.seq

    if (!doc) {
      console.log('skipping %s - invalid document (seq: %s)', change.id, seq)
      return cb()
    } else if (!doc.versions || doc.versions.length === 0) {
      console.log('skipping %s - no versions detected (seq: %s, modified: %s)', change.id, seq, modified)
      return cb()
    }

    var versions = Object.keys(doc.versions)
    while(versions.length) {
      var version = versions.shift()
      if (version) {
        var pkg = doc.versions[version]
        pkg._seq = seq
        pkg.id = change.id + '@' + version
        pushable.push(pkg)
      }
    }
    cb()
  }

}
