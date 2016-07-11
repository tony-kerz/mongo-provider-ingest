import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
//import axios from 'axios'
import Timer from './timer'
//import stringify from 'json-stringify-safe'
import minimist from 'minimist'

const dbg = debug('app:mongodb-geocoder')
const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const ADDRESS_KEY = 'addressKey'

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'
const source = argv.sourceCollection || 'cmsAggregatedLocations'
const target = argv.targetCollection || 'geocodedAddresses'
const thresh = argv.thresh || 100

let alreadyGeocodedCount = 0
let notGeocodedCount = 0

let query = {}

argv.city && (query = {...query, city: argv.city})
argv.state && (query = {...query, state: argv.state})
argv.zip && (query = {...query, zip: argv.zip})

async function run(url) {
  dbg('run: query=%o', query)

  const mainTimer = new Timer('main')
  mainTimer.start()

  try {
    const db = await client.connect(url)
    assert(db)

    db.collection(target).createIndex({[ADDRESS_KEY]: 1}, {unique: true})

    const count = await db.collection(source).count()
    const limit = argv.limit || count

    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

    const timer = new Timer('for-each')

    await db.collection(source).aggregate(
      [
        {$skip: argv.skip || 0},
        {$limit: limit},
        {$match: query},
        {
          $lookup: {
            from: target,
            localField: ADDRESS_KEY,
            foreignField: ADDRESS_KEY,
            as: 'geocoded'
          }
        },
        {$unwind: {path: '$geocoded', preserveNullAndEmptyArrays: true}},
        {
          // to-do: are native mongo ids much more efficient than strings?
          $group: {
            _id: `$${ADDRESS_KEY}`,
            addressLine1: {$last: '$addressLine1'},
            city: {$last: '$city'},
            state: {$last: '$state'},
            zip: {$last: '$zip'},
            geoPoint: {$last: '$geocoded.geoPoint'}
          }
        }
      ],
      {allowDiskUse: true}
    )
    .each((err, record)=>{
      assert.equal(null, err)
      if (record) {
        timer.start()
        //dbg('record=%o', record)
        if (record.geoPoint) {
          alreadyGeocodedCount++
        } else {
          notGeocodedCount++
        }
        timer.stop()
        if (timer.count() % thresh == 0) {
          // timer doesn't work correctly here because of async nature
          dbg('timer=%o', timer.toString())
        }
      } else {
        // possible to hit this before last record processed...?
        // may require little sleep here to avoid race...
        dbg('no more records...')
        db.close()
        mainTimer.stop()
        dbg('successfully processed [%o] records in [%s] seconds', limit, (mainTimer.total()/1000).toFixed(3))
        dbg('already-geocoded-count=%o, not-geocoded-count=%o', alreadyGeocodedCount, notGeocodedCount)
      }
    })
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
    process.exit(1)
  }
}

run(url)
