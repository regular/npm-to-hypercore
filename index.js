'use strict'

var ChangesStream = require('changes-stream')
var through2 = require('through2')
var pump = require('pump')
var level = require('level')
var path = require('path')
var Pushable = require('pull-pushable')

module.exports = function getStream(directory, seq) {
  var clean = require('normalize-registry-metadata')

  var db = level(path.join(directory, 'index'))
  var normalize = through2.obj(transform)
  var running = true 
  var pushable = Pushable(true, function(err) {
    running = false
  })
  
  run()
  return pushable.source

  function append(seq, doc, cb) {
    doc._seq = seq
    pushable.push(doc)
    cb(running ? null: new Error())
  }

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
      done()
      return
    } else if (!doc.versions || doc.versions.length === 0) {
      console.log('skipping %s - no versions detected (seq: %s, modified: %s)', change.id, seq, modified)
      done()
      return
    }

    var versions = Object.keys(doc.versions)
    processVersion()

    function done (err) {
      if (err) return cb(err)
      db.put('!latest_seq!', change.seq, cb)
    }

    function processVersion (err) {
      if (err) return done(err)
      var version = versions.shift()
      if (!version) return done()
      var key = change.id + '@' + version
      db.get(key, function (err) {
        if (!err || !err.notFound) return processVersion(err)
        append(change.seq, doc.versions[version], function (err) {
          if (err) return done(err)
          db.put(key, true, processVersion)
        })
      })
    }
  }

  function getNextSeqNo (cb) {
    db.get('!latest_seq!', function (err, seq) {
      if (err && err.notFound) cb(null, 0)
      else if (err) cb(err)
      else cb(null, parseInt(seq, 10) + 1)
    })
  }
}
