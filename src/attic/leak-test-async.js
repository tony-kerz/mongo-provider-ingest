import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'

const client = mongodb.MongoClient

const dbg = debug('app:provider-transformer')

async function run(url) {
  dbg('run: url=%o', url)

  try {
    const db = await client.connect(url)
    assert(db)

    // transform
    const cursor = db.collection('cmsProviderLocationsOriginal')
      .find({})
      .addCursorFlag('noCursorTimeout', true)
      .limit(100000)

    let idx = 0
    const thresh = 100000
    let source = null
    while (await cursor.hasNext()) {
      idx++
      source = await cursor.next()
      assert(source)
      if (idx % thresh == 0) {
        dbg('records-read=%o, heap-used=%o mb', idx, process.memoryUsage().heapUsed/1e6)
      }
    }
    cursor.close()
    global.gc()
    dbg('post-cursor-close: heap-used=%o mb', process.memoryUsage().heapUsed/1e6)
    db.close()
    global.gc()
    dbg('post-db-close: heap-used=%o mb', process.memoryUsage().heapUsed/1e6)
    global.gc()
    dbg('post-more-gc: heap-used=%o mb', process.memoryUsage().heapUsed/1e6)
    dbg('successfully xformed [%o] providers', idx)
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
    process.exit(1)
  }
}

run('mongodb://localhost:27017/test')
