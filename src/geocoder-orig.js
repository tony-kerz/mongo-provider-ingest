import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
import axios from 'axios'
import Timer from './timer'
//import stringify from 'json-stringify-safe'
import minimist from 'minimist'

const dbg = debug('app:mongodb-geocoder')
const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

let query = {
  loc: null
}

if (argv.state) {
  query = {
    ...query,
    'address.state': argv.state
  }
}

const collection = argv.collection || 'cmsLocations'

async function run(url) {
  dbg('run: query=%o', query)

  const mainTimer = new Timer('main')
  mainTimer.start()
  let idx = 0

  try {
    const db = await client.connect(url)
    assert(db)

    db.collection(collection).createIndex({geoPoint: '2dsphere'})

    const cursor = db.collection(collection)
      .find(query)
      .addCursorFlag('noCursorTimeout', true)
      .limit(argv.limit || 0)
      .skip(argv.skip || 0)

    const axiosTimer = new Timer('axios')
    const thresh = 100
    while (await cursor.hasNext()) {
      const provider = await cursor.next()
      //dbg('provider[%o]=%o', idx, toString(provider))
      const address = getAddress(provider)
      const params = {
        api_key: argv.apiKey || 'search-ND7BVJ',
        ['boundary.country']: 'USA',
        size: 1,
        text: address
      }
      axiosTimer.start()
      const geoResult = await axios.get('https://search.mapzen.com/v1/search', {params})
      axiosTimer.stop()
      if (axiosTimer.count() % thresh == 0) {
        dbg('axios-timer=%o', axiosTimer.toString())
      }
      //dbg('geo-result[%o]:', address)
      //dbg('%s', stringify(geoResult, null, 2))
      const coordinates = geoResult.data.features[0].geometry.coordinates
      //dbg('geo-result[%o]=%o', address, coordinates)
      const updateResult = await db.collection(collection).updateOne(
        {
          _id: provider._id
        },
        {
          $set: {
            geoPoint: {
              type: 'Point',
              coordinates
            }
          }
        }
      )
      assert(updateResult.modifiedCount == 1, 'expected modified-count == 1')
      idx++
    }
    db.close()
    mainTimer.stop()
    dbg('successfully geocoded [%o] providers in [%s] seconds', idx, (mainTimer.total()/1000).toFixed(3))
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
    process.exit(1)
  }
}

run(url)

function getAddress(p) {
  const {addressLine1, city, state, zip} = p.address
  return `${addressLine1} ${city}, ${state} ${zip}`
}
