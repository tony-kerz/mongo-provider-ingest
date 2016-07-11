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

  const source = argv.sourceCollection || 'cmsOriginalProviderLocations'
  const target = argv.targetCollection || 'cmsAggregatedProviders'

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    //db.collection(target).createIndex({npi: 1})

    const count = await db.collection(source).count()

    dbg('begin aggregation: source-count=%o', count)

    const result = await db.collection(source).aggregate(
      [
        {$limit: argv.limit || count},
        {
          $lookup: {
            from: 'npiOriginalProviderLocations',
            localField: 'npi',
            foreignField: 'NPI',
            as: 'npiProvider'
          }
        },
        {$unwind: '$npiProvider'},
        {
          $group: {
            _id: '$npi',
            firstName: {$last: '$firstName'},
            middleName: {$last: '$middleName'},
            lastName: {$last: '$lastName'},
            taxonomy1: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_1'},
            taxonomy2: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_2'},
            taxonomy3: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_3'},
            taxonomy4: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_4'},
            taxonomy5: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_5'},
            taxonomy6: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_6'},
            taxonomy7: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_7'},
            taxonomy8: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_8'},
            taxonomy9: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_9'},
            taxonomy10: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_10'},
            taxonomy11: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_11'},
            taxonomy12: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_12'},
            taxonomy13: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_13'},
            taxonomy14: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_14'},
            taxonomy15: {$push: '$npiProvider.Healthcare_Provider_Taxonomy_Code_15'}
          }
        },
        {
          $project: {
            _id: 0,
            npi: '$_id',
            firstName: 1,
            middleName: 1,
            lastName: 1,
            specialties: {
              '$setUnion': [
                '$taxonomy1',
                '$taxonomy2',
                '$taxonomy3',
                '$taxonomy4',
                '$taxonomy5',
                '$taxonomy6',
                '$taxonomy7',
                '$taxonomy8',
                '$taxonomy9',
                '$taxonomy10',
                '$taxonomy11',
                '$taxonomy12',
                '$taxonomy13',
                '$taxonomy14',
                '$taxonomy15'
              ]
            }
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
