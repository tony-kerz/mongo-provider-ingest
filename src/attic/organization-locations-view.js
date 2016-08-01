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

  const mainTimer = new Timer('main')
  mainTimer.start()

  const sourceName = argv.source || 'npiOrganizationLocationsView'
  const mergeName = argv.merge || 'cmsOrganizationLocationsView'
  const targetName = argv.target || 'organizationLocationsView'
  const query = argv.query ? JSON.parse(argv.query) : {}

  dbg('run args: url=%o, source=%o, merge=%o, target=%o, query=%o', url, sourceName, mergeName, targetName, query)

  try {
    const db = await client.connect(url)

    const source = db.collection(sourceName)
    const target = db.collection(targetName)

    source.createIndex({'anchor': 1})
    target.createIndex({'name': 1})
    target.createIndex({'identifiers.extension': 1})
    target.createIndex({'specialties.code': 1})
    target.createIndex({geoPoint: '2dsphere'})

    const count = await source.count()
    const limit = argv.limit || count
    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

    let result = await source.updateOne(
      {anchor: 'cms'},
      {$set: {anchor: 'cms'}},
      {upsert: true}
    )

    assert(result.result.n == 1)

    dbg('inserted anchor to [%o]', sourceName)

    result = await source.aggregate(
      [
        {$limit: limit},
        {
          $lookup: {
            from: mergeName,
            localField: 'anchor',
            foreignField: 'source',
            as: 'anchored'
          }
        },
        {$unwind: {path: '$anchored', preserveNullAndEmptyArrays: true}},
        {
          $project: {
            _id: 0,
            key: {$ifNull: ['$anchored.name', '$name']},
            source: {$ifNull: ['$anchored.source', '$source']}
          }
        },
        {$out: targetName}
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
      sourceName,
      targetName,
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
