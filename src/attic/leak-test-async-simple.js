import debug from 'debug'
import assert from 'assert'

const dbg = debug('app:leak-test-async')

async function run() {
  try {
    let idx = 0
    const thresh = 1e5
    const limit = 1e6
    while (idx < limit) {
      idx++
      const record = await get()
      assert(record)
      if (idx % thresh == 0) {
        dbg('records-read=%o, heap-used=%o mb', idx, process.memoryUsage().heapUsed/1e6)
      }
    }
    dbg('successfully processed [%o] records', idx)
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
  }
}

async function get() {
  return {
    foo: 'foo',
    bar: 'bar'
  }
}

run()
