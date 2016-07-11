import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
import Timer from './timer'
import _ from 'lodash'
//import stringify from 'json-stringify-safe'
import minimist from 'minimist'

const dbg = debug('app:provider-transformer')

const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')
  mainTimer.start()
  let idx = 0
  const metrics = {
    providerInsertCount: 0,
    providerUpdateCount: 0,
    locationInsertCount: 0,
    locationUpdateCount: 0,
    providerLocationUpsertCount :0
  }

  const skip = argv.skip || 0
  const limit = argv.limit || 100000
  const thresh = argv.thresh || 10000

  dbg('run args: url=%o, skip=%o, limit=%o, thresh=%o', url, skip, limit, thresh)

  try {
    const db = await client.connect(url)
    assert(db)

    // init
    if (skip == 0) {
      dbg('initializating for first pass (skip=%o)', skip)

      // clean
      _.forEach(
        ['cmsProviders', 'cmsLocations', 'cmsProviderLocations'],
        (c) => {
          db.dropCollection(c)
        }
      )

      // indexes
      db.collection('cmsProviders').createIndex({npi: 1}, {unique: true})
      db.collection('cmsLocations').createIndex(
        {
          orgKey: 1,
          'address.addressLine1': 1,
          'address.city': 1,
          'address.state': 1,
          'address.zip': 1
        },
        {unique: true}
      )
      db.collection('cmsProviderLocations').createIndex({providerId: 1, locationId: 1}, {unique: true})
    }

    // transform
    const cursor = db.collection('cmsProviderLocationsOriginal')
      .find({})
      .addCursorFlag('noCursorTimeout', true)
      .skip(skip)
      .limit(limit)

    const timer = new Timer('xform')
    while (await cursor.hasNext()) {
      timer.start()
      const source = await cursor.next()
      //dbg('source[%o]=%o', idx, toString(source))

      // upsert provider
      //
      const providerKey = {npi: source.npi}
      let provider = await db.collection('cmsProviders').findOne(providerKey)
      if (provider) {
        //dbg('found npi=%o', provider.npi)
        // same provider diff location
        // add to it's location array
        metrics.providerUpdateCount++
      } else {
        provider = {
          ...providerKey,
          firstName: source.firstName,
          lastName: source.lastName,
          middleName: source.middleName,
          specialties: [
            source.specialty
          ]
        }
        const result = await db.collection('cmsProviders').insertOne(provider)
        assert.equal(result.insertedCount, 1)
        metrics.providerInsertCount++
      }

      // upsert location
      //
      const locationKey = {
        orgKey: source.groupPac || source.npi,
        address: getAddress(source)
      }
      let location = await db.collection('cmsLocations').findOne(locationKey)
      if (location) {
        //dbg('found location=%o', locationKey)
        // same location diff provider
        // add to it's provider array
        metrics.locationUpdateCount++
      } else {
        location = {
          ...locationKey,
          orgName: source.orgName || getFullName(source),
          phone: source.phone
        }
        const result = await db.collection('cmsLocations').insertOne(location)
        assert.equal(result.insertedCount, 1)
        metrics.locationInsertCount++
      }

      // upsert provider-location
      const providerLocationKey = {
        providerId: provider._id,
        locationId: location._id
      }
      const result = await db.collection('cmsProviderLocations').updateOne(
        providerLocationKey,
        {
          ...providerLocationKey,
          npi: source.npi,
          ...locationKey
        },
        {
          upsert: true
        }
      )
      //dbg('result=%o', result)
      assert((result.modifiedCount == 1) || (result.upsertedCount == 1))
      metrics.providerLocationUpsertCount++
      timer.stop()
      if (timer.count() % thresh == 0) {
        dbg('timer=%o', timer.toString())
        dbg('heap-used=%o mb', process.memoryUsage().heapUsed/1e6)
      }
      idx++
    }
    db.close()
    mainTimer.stop()
    dbg('successfully xformed [%o] providers in [%s] seconds, metrics=%o', idx, (mainTimer.total()/1000).toFixed(3), metrics)
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
    process.exit(1)
  }
}

run(url).then(()=>{
  dbg('run: completed...')
})

function getAddress(record) {
  // ignore addressLine2
  return {
    addressLine1: record.addressLine1,
    city: record.city,
    state: record.state,
    zip: record.zip
  }
}

function getFullName(o) {
  return `${o.lastName}, ${o.firstName}${o.middleName && ` ${o.middleName}`}`
}
// function toString(p) {
//   return `npi=${p.npi}, name=${p.lastName}, ${p.firstName} ${p.middleName || ''}`
// }
