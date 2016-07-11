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

const ADDRESS_KEY = 'addressKey'

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')
  mainTimer.start()

  const source = argv.sourceCollection || 'cmsAggregatedLocations'
  const target = argv.targetCollection || 'geocodedAddresses'

  // the idea behind this routine is to preserve geocodes at target

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    db.collection(target).createIndex({locationKey: 1})

    const count = await db.collection(source).count()

    dbg('begin aggregation: source-count=%o', count)

    const result = await db.collection(source).aggregate(
      [
        //{$limit: 1000},
        {
          $lookup: {
            from: target,
            localField: ADDRESS_KEY,
            foreignField: ADDRESS_KEY,
            as: 'preserve'
          }
        },
        //{$unwind: '$preserve'},
        {
          // to-do: are native mongo ids much more efficient than strings?
          $group: {
            _id: `$${ADDRESS_KEY}`,
            addressLine1: {$last: '$addressLine1'},
            city: {$last: '$city'},
            state: {$last: '$state'},
            zip: {$last: '$zip'},
            geoPoint: {$last: '$preserve.geoPoint'}
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
