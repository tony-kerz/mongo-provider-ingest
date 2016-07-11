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

  const source = argv.sourceCollection || 'cmsLocations'
  const target = argv.targetCollection || 'geocodedAddresses'

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    db.collection(target).createIndex({addressKey: 1}, {unique: true})

    const count = await db.collection(source).count()

    dbg('begin aggregation: source-count=%o', count)

    const result = await db.collection(source).aggregate(
      [
        {$match: {geoPoint: {$ne: null}}},
        {
          $group: {
            _id: {
              addressLine1: '$address.addressLine1',
              city: '$address.city',
              state: '$address.state',
              zip: '$address.zip'
            },
            geoPoint: {$last: '$geoPoint'}
          }
        },
        {
          $project: {
            _id: 0,
            addressLine1: '$_id.addressLine1',
            city: '$_id.city',
            state: '$_id.state',
            zip: '$_id.zip',
            addressKey: {
              $concat: [
                {$substr: ['$_id.addressLine1',0,-1]},
                ':',
                '$_id.city',
                ':',
                '$_id.state',
                ':',
                {$substr: ['$_id.zip', 0, -1]}
              ]
            },
            geoPoint: 1
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
