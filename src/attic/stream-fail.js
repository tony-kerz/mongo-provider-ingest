import debug from 'debug'
import assert from 'assert'
//import Timer from './timer'
//import stringify from 'json-stringify-safe'
import fs from 'fs'
import parse from 'csv-parse'
import mongodb from 'mongodb'

const dbg = debug('app:provider-ingest')
const input = fs.createReadStream('./src/providers.csv')
const parser = parse({columns: true})
const client = mongodb.MongoClient

parser.on('readable', () => {
  ingest('mongodb://localhost:27017/test').then(() => {
    dbg('ingest.then')
  })
  return null
})

parser.on('error', (err) => {
  dbg('err=%o', err)
})

parser.on('finish', () => {
  dbg('done...')
})

async function ingest(url) {
  try {
    const db = await client.connect(url)
    assert(db, 'db required')
    let record
    let count = 0
    // eslint-disable-next-line no-cond-assign
    while (record = parser.read()) {
      count++
      dbg('ingest: record=%o', record)
      if (count == 10) {
        return null
      }
    }
  } catch (caught) {
    dbg('caught=%o', caught)
  }
  return null
}

input.pipe(parser)
