import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'

const client = mongodb.MongoClient

const dbg = debug('app:provider-transformer')

process.on('unhandledRejection', (err)=>{
  dbg('unhandled-rejection: %o', err)
  process.exit(1)
})

function run(url) {
  dbg('run: url=%o', url)

  client.connect(url)
  .then((db)=>{
    dbg('connect-then: db=%o', db)
    assert(db, 'db-required')

    let idx = 0
    const thresh = 1e5
    const limit = 1e6
    //const cursor = db.collection('cmsProviderLocationsOriginal')
    db.collection('cmsProviderLocationsOriginal')
      .find({})
      .addCursorFlag('noCursorTimeout', true)
      .limit(limit)
      //.then((a, b)=> {
      //  dbg('find-then... a=%o, b=%o', a, b)
      //})
      .forEach((source)=>{
        //dbg('for-each: source=%o', source)
        assert(source, 'source required')
        idx++
        if (idx % thresh == 0) {
          dbg('records-read=%o, heap-used=%o mb', idx, process.memoryUsage().heapUsed/1e6)
        }
      })
      // .then(()=>{
      //   dbg('for-each: then...')
      //   db.close()
      //   dbg('successfully xformed [%o] providers', idx)
      //   process.exit(1)
      // })
    // cursor.hasNext().then((err, source)=>{
    //   dbg('has-next-then: err=%o, source=%o', err, source)
    //   assert.equal(err, null, `has-next: error: ${err}`)
    //   if (source) {
    //     idx++
    //     if (idx % thresh == 0) {
    //       dbg('records-read=%o, heap-used=%o mb', idx, process.memoryUsage().heapUsed/1e6)
    //     }
    //   } else {
    //     db.close()
    //     dbg('successfully xformed [%o] providers', idx)
    //   }
    // })
  })
}

run('mongodb://localhost:27017/test')
