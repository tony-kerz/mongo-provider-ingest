import debug from 'debug'
import mongodb from 'mongodb'
import Timer from './timer'
import assert from 'assert'
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

  const source = argv.sourceCollection || 'cmsProviderLocations'
  const target = argv.targetCollection || 'cmsProviderLocationsView'

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    db.collection(target).createIndex({npi: 1})
    db.collection(target).createIndex({firstName: 1})
    db.collection(target).createIndex({lastName: 1})
    //db.collection(target).createIndex({geoPoint: '2dsphere'})

    const count = await db.collection(source).count()

    dbg('begin aggregation: source-count=%o', count)

    const result = await db.collection(source).aggregate(
      [
        {$limit: argv.limit || count},
        {
          $lookup: {
            from: 'npiProviders',
            localField: 'npi',
            foreignField: 'npi',
            as: 'provider'
          }
        },
        {
          $lookup: {
            from: 'cmsLocations',
            localField: 'locationKey',
            foreignField: 'locationKey',
            as: 'location'
          }
        },
        {$unwind: '$provider'},
        {$unwind: '$location'},
        {
          $project: {
            npi: '$provider.npi',
            firstName: '$provider.firstName',
            middleName: '$provider.middleName',
            lastName: '$provider.lastName',
            specialties: '$provider.specialties',
            orgName: '$location.orgName',
            address: {
              line1: '$location.addressLine1',
              city: '$location.city',
              state: '$location.state',
              zip: '$location.zip'
            },
            phone: '$location.phone',
            addressKey: '$location.addressKey'
          }
        },
        {
          $lookup: {
            from: 'geocodedAddresses',
            localField: 'addressKey',
            foreignField: 'addressKey',
            as: 'geocoded'
          }
        },
        {$unwind: {path: '$geocoded', preserveNullAndEmptyArrays: true}},
        {
          $project: {
            npi: 1,
            firstName: 1,
            middleName: 1,
            lastName: 1,
            specialties: 1,
            orgName: 1,
            address: 1,
            phone: 1,
            geoPoint: '$geocoded.geoPoint'
          }
        },
        {$out: target}
      ]
    )
    .toArray()

    assert(result)

    db.close()
    mainTimer.stop()
    dbg(
      'successfully aggregated [%o] records from [%s] to [%s] in [%s] seconds',
      count,
      source,
      target,
      (mainTimer.total()/1000).toFixed(3)
    )
  }
  catch (caught) {
    dbg('connect: caught=%o', caught)
    process.exit(1)
  }
}

run(url).then(()=>{
  dbg('run: completed...')
})
