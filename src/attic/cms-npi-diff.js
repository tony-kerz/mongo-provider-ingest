import debug from 'debug'
import mongodb from 'mongodb'
import Timer from './timer'
import assert from 'assert'
//import stringify from 'json-stringify-safe'
import minimist from 'minimist'

const dbg = debug('app:cms-providers')

const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')
  mainTimer.start()

  const source = argv.sourceCollection || 'cmsAggregatedProviders'
  const target = argv.targetCollection || 'cmsNpiJoin'

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    //db.collection(target).createIndex({npi: 1})

    const count = await db.collection(source).count()

    dbg('begin aggregation: source-count=%o', count)

    const result = await db.collection(source).aggregate(
      [
        //{$limit: 1000},
        {
          $lookup: {
            from: 'npiOriginalProviderLocations',
            localField: 'npi',
            foreignField: 'NPI',
            as: 'npiProviders'
          }
        },
        {
          $project: {
            npi: 1,
            npiNpi: '$npiProviders.NPI'
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
