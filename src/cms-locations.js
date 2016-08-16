import debug from 'debug'
import mongodb from 'mongodb'
import Timer from 'tymer'
import assert from 'assert'
import minimist from 'minimist'

const dbg = debug('app:cms-locations')

const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

const SEPARATOR = ':'

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')
  mainTimer.start()

  const source = argv.sourceCollection || 'cmsOriginalProviderLocations'
  const target = argv.targetCollection || 'cmsLocations'

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    db.collection(target).createIndex({locationKey: 1})

    const count = await db.collection(source).count()
    const limit = argv.limit || count

    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

    // note: substr[blah, 0, -1] to convert number to string (better way?)

    const addressKeyArray = [
      {$substr: ['$_id.addressLine1', 0, -1]},
      SEPARATOR,
      '$_id.city',
      SEPARATOR,
      '$_id.state',
      SEPARATOR,
      {$substr: ['$_id.zip', 0, -1]}
    ]

    const result = await db.collection(source).aggregate(
      [
        {$limit: limit},
        {
          $group: {
            _id: {
              orgKey: {$ifNull: ['$groupPac', '$npi']},
              addressLine1: '$addressLine1',
              city: '$city',
              state: '$state',
              zip: '$zip'
            },
            orgName: {$last: '$orgName'},
            phone: {$last: {$substr: ['$phone', 0, -1]}}
          }
        },
        {
          $project: {
            _id: 0,
            locationKey: {
              $concat: [
                {$substr: ['$_id.orgKey', 0, -1]},
                SEPARATOR
              ].concat(addressKeyArray)
            },
            orgKey: {$substr: ['$_id.orgKey', 0, -1]},
            addressKey: {$concat: addressKeyArray},
            addressLine1: '$_id.addressLine1',
            city: '$_id.city',
            state: '$_id.state',
            zip: '$_id.zip',
            orgName: 1,
            //practitioners: 1,
            phone: 1
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
