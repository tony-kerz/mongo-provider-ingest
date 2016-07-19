import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
import Timer from 'tymer'
import geocode from 'geocodr'
import minimist from 'minimist'

const dbg = debug('app:mongodb-geocoder')
const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const ADDRESS_KEY = 'addressKey'

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'
const source = argv.sourceCollection || 'cmsLocations'
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

  try {
    const db = await client.connect(url)
    assert(db)

    db.collection(target).createIndex({[ADDRESS_KEY]: 1}, {unique: true})

    const count = await db.collection(source).count()
    const limit = argv.limit || count

    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

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
    .each(async (err, record)=>{
      assert.equal(null, err)
      if (record) {
        if (record.geoPoint) {
          alreadyGeocodedCount++
        } else {
          const timer = new Timer()
          notGeocodedCount++
          const coordinates = await geocode(getAddress(record))
          dbg('coordinates=%o', coordinates)
          // insert new doc into target
          const result = await db.collection(target).insert(
            {
              geoPoint: {type: 'Point', coordinates},
              addressLine1: record.addressLine1,
              city: record.city,
              state: record.state,
              zip: record.zip,
              addressKey: record.addressKey
            }
          )
          dbg('result=%o', result)
          assert(result.modifiedCount == 1, 'expected inserted-count == 1')
          timer.stop()
          mainTimer.record(timer.last())
        }
        if (mainTimer.count() % thresh == 0) {
          dbg('timer=%o', mainTimer.toString())
        }
      } else {
        // possible to hit this before last record processed...?
        // may require little sleep here to avoid race...
        dbg('no more records...')
        db.close()
        dbg('successfully processed [%o] records in [%s] seconds', mainTimer.count(), (mainTimer.total()/1000).toFixed(3))
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

function getAddress(r) {
  return `${r.addressLine1} ${r.city}, ${r.state} ${r.zip}`
}
