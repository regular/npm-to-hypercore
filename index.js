'use strict'

var ChangesStream = require('changes-stream')
var through2 = require('through2')
var pump = require('pump')
var path = require('path')
var pull = require('pull-stream')
var Pushable = require('pull-pushable')
var clean = require('normalize-registry-metadata')

module.exports = function getStream(seq) {

  var normalize = through2.obj(transform)
  var running = true 
  var pushable = Pushable(true, function(err) {
    running = false
  })
  run()

  return pull(
    pushable.source,
    pull.filter( (change)=>{
      if (!change.doc) {
        console.error('skipping %s - invalid document (seq: %s)', change.id, change.seq)
        return false
      }
      return true
    }),
    pull.filter( (change)=>{
      // returns undefined id doc is invalid
      return clean(change.doc)
    }),
    pull.filter( (change)=>{
      var doc = change.doc
      if (!doc.versions || doc.versions.length === 0) {
        console.error('skipping %s - no versions detected (seq: %s)', change.id, change.seq)
        return false
      }
      return true
    }),
    pull.map( (change)=>{
      var doc = change.doc
      var modified = doc && doc.time && doc.time.modified
      return Object.keys(doc.versions).map( (version)=>{
        var pkg = doc.versions[version]
        pkg._seq = change.seq
        pkg._modified = modified
        pkg.id = change.id + '@' + version
        return pkg
      })
    }),
    pull.flatten(),
    pull.filter()
  )

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
    pushable.push(change)
    cb(running ? null : new Error('Stream aborted'))
  }

}
