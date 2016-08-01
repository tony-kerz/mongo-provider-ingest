import debug from 'debug'
import mongodb from 'mongodb'
import Timer from 'tymer'
import assert from 'assert'
import minimist from 'minimist'

const dbg = debug('app:cms-organization-locations-view')

const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')
  mainTimer.start()

  const source = argv.sourceCollection || 'cmsProviderLocations'
  const target = argv.targetCollection || 'cmsOrganizationLocationsView'
  const query = argv.query ? JSON.parse(argv.query) : {}
  const socketTimeoutSeconds = argv.socketTimeoutSeconds || 0

  dbg('run args: url=%o, source=%o, target=%o, query=%o', url, source, target, query)

  try {
    const db = await client.connect(url, {server: {socketOptions: {socketTimeoutMS: socketTimeoutSeconds * 1000}}})

    db.collection(target).createIndex({'name': 1})
    db.collection(target).createIndex({'identifiers.extension': 1})
    db.collection(target).createIndex({'specialties.code': 1})
    db.collection(target).createIndex({geoPoint: '2dsphere'})

    const count = await db.collection(source).count()
    const limit = argv.limit || count
    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

    const result = await db.collection(source).aggregate(
      [
        {$limit: limit},
        {
          $lookup: {
            from: 'npiProviders',
            localField: 'npi',
            foreignField: 'npi',
            as: 'provider'
          }
        },
        {$unwind: '$provider'},
        {
          $lookup: {
            from: 'cmsLocations',
            localField: 'locationKey',
            foreignField: 'locationKey',
            as: 'location'
          }
        },
        {$unwind: '$location'},
        {$match: {'location.orgName': {$ne: null}}},
        {$match: query},
        {
          $lookup: {
            from: 'geocodedAddresses',
            localField: 'location.addressKey',
            foreignField: 'addressKey',
            as: 'geocoded'
          }
        },
        {$unwind: {path: '$geocoded', preserveNullAndEmptyArrays: true}},
        {$unwind: '$provider.specialties'},
        {
          $lookup: {
            from: 'taxonomy',
            localField: 'provider.specialties',
            foreignField: 'Code',
            as: 'taxonomy'
          }
        },
        {$unwind: '$taxonomy'},
        {
          $group: {
            _id: '$location.locationKey',
            doc: {$last: '$$ROOT'},
            practitioners: {
              $addToSet: {
                name: {
                  first: '$provider.firstName',
                  last: '$provider.lastName'
                }
              }
            },
            specialties: {
              $addToSet: {
                code: '$provider.specialties',
                text: '$taxonomy.Classification',
                system: {$literal: '2.16.840.1.113883.6.101'}
              }
            }
          }
        },
        {
          $project: {
            _id: 0,
            name: {$substr: ['$doc.location.orgName', 0, -1]},
            identifiers: [
              {
                authority: {$literal: 'CMS'},
                oid: {$literal: '2.16.840.1.113883.4.6'},
                extension: '$doc.location.orgKey'
              }
            ],
            practitioners: 1,
            specialties: 1,
            address: {
              line1: '$doc.location.addressLine1',
              city: '$doc.location.city',
              state: '$doc.location.state',
              zip: '$doc.location.zip'
            },
            phone: '$doc.location.phone',
            geoPoint: '$doc.geocoded.geoPoint',
            source: {$literal: 'cms'}
          }
        },
        {$out: target}
      ],
      {allowDiskUse: true}
    )
    .toArray()

    assert(result)

    db.close()
    mainTimer.stop()
    dbg(
      'successfully aggregated [%o] records from [%s] to [%s] in [%s] seconds',
      limit,
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
