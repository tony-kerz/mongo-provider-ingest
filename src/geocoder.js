import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
import Timer from 'tymer'
import geocode from 'geocodr'
import minimist from 'minimist'

const dbg = debug('app:geocoder')
const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const ADDRESS_KEY = 'addressKey'
const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'
const source = argv.sourceCollection || 'cmsLocations'
const target = argv.targetCollection || 'geocodedAddresses'
const thresh = argv.thresh || 100
const sleepMillis = argv.sleepMillis || 0

async function run(url) {
  try {
    const timer = new Timer('main')
    const db = await client.connect(url)
    assert(db)

    db.collection(target).createIndex({[ADDRESS_KEY]: 1}, {unique: true})

    const limit = argv.limit || 30000
    const query = argv.query ? JSON.parse(argv.query) : {}

    dbg('begin aggregation: query=%o, limit=%o, sleep-millis=%o', query, limit, sleepMillis)

    const targets = await db.collection(source).aggregate(
      [
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
        {$match: {'geocoded.geoPoint': null}},
        {
          $group: {
            _id: `$${ADDRESS_KEY}`,
            addressLine1: {$last: '$addressLine1'},
            city: {$last: '$city'},
            state: {$last: '$state'},
            zip: {$last: '$zip'},
            geoPoint: {$last: '$geocoded.geoPoint'}
          }
        },
        {$limit: limit}
      ],
      {allowDiskUse: true}
    )
    .toArray()

    dbg('targets.length=%o', targets.length)

    for (let i = 0; i < targets.length; i++) {
      timer.start()
      const record = targets[i]
      const address = getAddress(record)
      //dbg('attempting to geocode address=%o', address)
      const coordinates = await geocode(address)
      if (coordinates) {
        const result = await db.collection(target).insertOne(
          {
            geoPoint: {type: 'Point', coordinates},
            addressLine1: record.addressLine1,
            city: record.city,
            state: record.state,
            zip: record.zip,
            addressKey: record._id
          }
        )
        assert.equal(result.insertedCount, 1)
        timer.stop()
        if (timer.count() % thresh == 0) {
          dbg('timer=%o', timer.toString())
        }
        sleep(sleepMillis)
      } else {
        dbg('unable to geocode address [%o], skipping...', address)
      }
    }

    dbg('successfully processed [%o] records in [%s] seconds', timer.count(), (timer.total()/1000).toFixed(3))
    db.close()
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

function sleep(s) {
  const e = new Date().getTime() + (s)
  while (new Date().getTime() <= e) {
    // eslint-disable-next-line no-extra-semi
    ;
  }
}
