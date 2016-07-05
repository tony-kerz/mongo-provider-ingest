import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
import Timer from './timer'
import _ from 'lodash'
//import stringify from 'json-stringify-safe'

const client = mongodb.MongoClient

const dbg = debug('app:provider-transformer')

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')

  const metrics = {
    providerInsertCount: 0,
    providerUpdateCount: 0,
    locationInsertCount: 0,
    locationUpdateCount: 0,
    providerLocationUpsertCount :0
  }
  try {
    const db = await client.connect(url)
    assert(db)

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

    const timer = new Timer('xform')
    //const limit = 1000
    //const thresh = 100
    const limit = await db.collection('cmsProviderLocationsOriginal').count()
    const thresh = 10000

    dbg('limit=%o, thresh=%o', limit, thresh)

    // transform
    db.collection('cmsProviderLocationsOriginal')
      .find({})
      .addCursorFlag('noCursorTimeout', true)
      .batchSize(100)
      .limit(limit)
      .forEach(
        (source)=>{
          assert(source)
          //dbg('source=%o', source)
          timer.start()

          // upsert provider
          //
          const providerKey = {npi: source.npi}
          const provider = {
            ...providerKey,
            firstName: source.firstName,
            lastName: source.lastName,
            middleName: source.middleName,
            specialties: [
              source.specialty
            ]
          }

          db.collection('cmsProviders').updateOne(
            providerKey,
            provider,
            {upsert: true},
            (err, result)=>{
              //dbg('providers: update-one: source=%o', source)
              if (err && err.code == 11000) {
                dbg('ignoring upsert error=%o', err)
                return null
                //const id = await db.collection('cmsProviders').findOne(providerKey)
              } else {
                assert.equal(err, null)
              }
              assert((result.modifiedCount == 1) || (result.upsertedCount == 1))
              // check res for up v in
              metrics.providerInsertCount++
              provider._id = result.upsertedId

              // upsert location
              //
              const locationKey = {
                orgKey: source.groupPac || source.npi,
                address: getAddress(source)
              }
              const location = {
                ...locationKey,
                orgName: source.orgName || getFullName(source),
                phone: source.phone
              }

              db.collection('cmsLocations').updateOne(
                locationKey,
                location,
                {upsert: true},
                (err, result)=>{
                  //dbg('locations: update-one: provider=%o', provider)
                  assert.equal(err, null)
                  assert((result.modifiedCount == 1) || (result.upsertedCount == 1))
                  // check res for up v in
                  metrics.locationInsertCount++
                  location._id = result.upsertedId

                  // upsert providerLocations
                  //
                  const providerLocationKey = {
                    providerId: provider._id,
                    locationId: location._id
                  }
                  //dbg('provider-location-key: %o', providerLocationKey)
                  db.collection('cmsProviderLocations').updateOne(
                    providerLocationKey,
                    {
                      ...providerLocationKey,
                      npi: source.npi,
                      ...locationKey
                    },
                    {upsert: true},
                    (err, result)=>{
                      assert.equal(err, null)
                      //dbg('provider-locations: update-one: err=%o, result=%o', err, result)
                      assert((result.modifiedCount == 1) || (result.upsertedCount == 1))
                      metrics.providerLocationUpsertCount++
                      timer.stop()
                      if (timer.count() % thresh == 0) {
                        dbg('timer=%o', timer.toString())
                        dbg('heap-used=%o mb', process.memoryUsage().heapUsed/1e6)
                      }
                      if (timer.count() == limit) {
                        db.close()
                        mainTimer.stop()
                        dbg(
                          'successfully xformed [%o] providers in [%s] seconds, metrics=%o',
                          limit,
                          (mainTimer.total()/1000).toFixed(3),
                          metrics
                        )
                      }
                    }
                  )
                }
              )
            }
          )
        },
        (err)=>{
          assert.equal(err, null)
          dbg('finished reading records, waiting for processing to complete...')
        }
      )
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
    process.exit(1)
  }
}

run('mongodb://localhost:27017/test')

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
