import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'

const client = mongodb.MongoClient

const dbg = debug('app:provider-transformer')

function run(url) {
  dbg('run: url=%o', url)

  client.connect(url, (err, db)=>{
    assert.equal(err, null)
    let idx = 0
    const thresh = 1e5
    db.collection('cmsProviderLocationsOriginal').find({})
      .addCursorFlag('noCursorTimeout', true)
      //.limit(100)
      .forEach(
        (source)=>{
          assert(source)
          idx++
          if (idx % thresh == 0) {
            dbg('records-read=%o, heap-used=%o mb', idx, process.memoryUsage().heapUsed/1e6)
          }
        },
        (err)=>{
          assert.equal(err, null)
          db.close()
          dbg('successfully xformed [%o] providers', idx)
        }
      )
  })
}

run('mongodb://localhost:27017/test')
