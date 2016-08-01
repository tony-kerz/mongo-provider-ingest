import debug from 'debug'
import assert from 'assert'
import mongodb from 'mongodb'
import Timer from 'tymer'
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

    const source = argv.sourceCollection || 'cmsProviderLocationsView'
    const target = argv.targetCollection || 'cmsOrganizationLocationsView'
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
        {
          $group: {
            _id: {
              orgName: {$substr: ['$orgName', 0, -1]},
              addressLine1: '$address.line1',
              city: '$address.city',
              state: '$address.state',
              zip: '$address.zip'
            },
            specialties: {$push: '$specialties'},
            practitioners: {$push: {
              last: '$name.last',
              first: '$name.first'
            }},
            doc: {$last: '$$ROOT'}
          }
        },
        {$limit: limit},
        {
          $project: {
            _id: 0,
            orgName: '$doc.orgName',
            identifiers: [
              {
                authority: {$literal: 'CMS'},
                oid: {$literal: '2.16.840.1.113883.4.6'},
                extension: '$doc.orgKey'
              }
            ],
            address: '$doc.address',
            specialties: {$setUnion: '$specialties'},
            practitioners: 1,
            phone: '$doc.phone'
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
