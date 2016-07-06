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

  const collection = argv.url || 'cmsProviderLocations'

  dbg('run args: url=%o, collection=%o', url, collection)

  try {
    const db = await client.connect(url)

    const count = await db.collection(collection).count()

    dbg('begin aggregation: count=%o', count)

    const result = await db.collection(collection).aggregate(
      [
        {
          $lookup: {
            from: 'cmsProviders',
            localField: 'providerId',
            foreignField: '_id',
            as: 'provider'
          }
        },
        {
          $lookup: {
            from: 'cmsLocations',
            localField: 'locationId',
            foreignField: '_id',
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
            address: '$location.address',
            phone: '$location.phone'
          }
        },
        {$out: 'cmsDenormedProviderLocations'}
      ]
    )
    .toArray()

    assert(result)

    db.close()
    mainTimer.stop()
    dbg(
      'successfully aggregated [%o] provider-locations in [%s] seconds',
      count,
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
