import debug from 'debug'
import mongodb from 'mongodb'
import Timer from 'tymer'
import assert from 'assert'
import minimist from 'minimist'

const dbg = debug('app:organization-locations-view')

const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

async function run(url) {
  dbg('run: url=%o', url)
  try {
    const mainTimer = new Timer('main')
    mainTimer.start()

    const source = argv.sourceCollection || 'npiOrganizations'
    const target = argv.targetCollection || 'npiOrganizationLocationsView'
    const query = argv.query ? JSON.parse(argv.query) : {}

    dbg('run args: url=%o, source=%o, target=%o, query=%o', url, source, target, query)

    const db = await client.connect(url)

    db.collection(target).createIndex({'name': 1})
    db.collection(target).createIndex({'identifiers.extension': 1})
    db.collection(target).createIndex({'specialties.code': 1})
    db.collection(target).createIndex({geoPoint: '2dsphere'})

    const count = await db.collection(source).count()
    const limit = argv.limit || count
    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

    const result = await db.collection(source).aggregate(
      [
        {$match: query},
        {$limit: limit},
        {
          $lookup: {
            from: 'geocodedAddresses',
            localField: 'addressKey',
            foreignField: 'addressKey',
            as: 'geocoded'
          }
        },
        {$unwind: {path: '$geocoded', preserveNullAndEmptyArrays: true}},
        {$unwind: '$specialties'},
        {
          $lookup: {
            from: 'taxonomy',
            localField: 'specialties',
            foreignField: 'Code',
            as: 'taxonomy'
          }
        },
        {$unwind: '$taxonomy'},
        {
          $group: {
            _id: '$_id',
            doc: {$last: '$$ROOT'},
            specialties: {
              $push: {
                code: '$specialties',
                text: '$taxonomy.Classification',
                system: {$literal: '2.16.840.1.113883.6.101'}
              }
            }
          }
        },
        {
          $project: {
            name: '$doc.name',
            identifiers: [
              {
                authority: {$literal: 'CMS'},
                oid: {$literal: '2.16.840.1.113883.4.6'},
                extension: '$doc.npi'
              }
            ],
            specialties: 1,
            address: {
              line1: '$doc.addressLine1',
              city: '$doc.city',
              state: '$doc.state',
              zip: '$doc.zip'
            },
            phone: '$doc.phone',
            geoPoint: '$doc.geocoded.geoPoint',
            source: {$literal: 'npi'}
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
