import debug from 'debug'
import mongodb from 'mongodb'
import Timer from 'tymer'
import assert from 'assert'
import minimist from 'minimist'
import {getAddressKeyArray, getLocationKeyArray} from './shared/helper'

const dbg = debug('app:npi-organizations')

const argv = minimist(process.argv.slice(2))
dbg('argv=%o', argv)

const client = mongodb.MongoClient
const url = argv.url || 'mongodb://localhost:27017/test'

async function run(url) {
  dbg('run: url=%o', url)

  const mainTimer = new Timer('main')
  mainTimer.start()

  const source = argv.sourceCollection || 'npiOriginalProviderLocations'
  const target = argv.targetCollection || 'npiOrganizations'

  dbg('run args: url=%o, source=%o, target=%o', url, source, target)

  try {
    const db = await client.connect(url)

    db.collection(target).createIndex({npi: 1})

    const count = await db.collection(source).count()
    const limit = argv.limit || count

    dbg('begin aggregation: source-count=%o, limit=%o', count, limit)

    const addressKeyArray = getAddressKeyArray(
      '$addressLine1',
      '$city',
      '$state',
      '$zip'
    )

    const result = await db.collection(source).aggregate(
      [
        {
          $match: {
            Entity_Type_Code: 2,
            $or: [
              {NPI_Deactivation_Date: null},
              {NPI_Reactivation_Date: {$ne: null}}
            ]
          }
        },
        {$limit: limit},
        {
          $group: {
            _id: '$NPI',
            name: {$last: '$Provider_Organization_Name'},
            addressLine1: {$last: '$Provider_First_Line_Business_Practice_Location_Address'},
            city: {$last: '$Provider_Business_Practice_Location_Address_City_Name'},
            state: {$last: '$Provider_Business_Practice_Location_Address_State_Name'},
            zip: {$last: '$Provider_Business_Practice_Location_Address_Postal_Code'},
            phone: {$last: '$Provider_Business_Practice_Location_Address_Telephone_Number'},
            taxonomy1: {$push: '$Healthcare_Provider_Taxonomy_Code_1'},
            taxonomy2: {$push: '$Healthcare_Provider_Taxonomy_Code_2'},
            taxonomy3: {$push: '$Healthcare_Provider_Taxonomy_Code_3'},
            taxonomy4: {$push: '$Healthcare_Provider_Taxonomy_Code_4'},
            taxonomy5: {$push: '$Healthcare_Provider_Taxonomy_Code_5'},
            taxonomy6: {$push: '$Healthcare_Provider_Taxonomy_Code_6'},
            taxonomy7: {$push: '$Healthcare_Provider_Taxonomy_Code_7'},
            taxonomy8: {$push: '$Healthcare_Provider_Taxonomy_Code_8'},
            taxonomy9: {$push: '$Healthcare_Provider_Taxonomy_Code_9'},
            taxonomy10: {$push: '$Healthcare_Provider_Taxonomy_Code_10'},
            taxonomy11: {$push: '$Healthcare_Provider_Taxonomy_Code_11'},
            taxonomy12: {$push: '$Healthcare_Provider_Taxonomy_Code_12'},
            taxonomy13: {$push: '$Healthcare_Provider_Taxonomy_Code_13'},
            taxonomy14: {$push: '$Healthcare_Provider_Taxonomy_Code_14'},
            taxonomy15: {$push: '$Healthcare_Provider_Taxonomy_Code_15'}
          }
        },
        {
          $project: {
            _id: 0,
            npi: '$_id',
            name: 1,
            addressLine1: 1,
            city: 1,
            state: 1,
            zip: 1,
            phone: 1,
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
            },
            addressKey: {$concat: addressKeyArray},
            locationKey: {$concat: getLocationKeyArray('$_id', addressKeyArray)}
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
