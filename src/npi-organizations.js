import getIngester from './shared/get-ingester'
import {getAddressKeyArray, getLocationKeyArray} from './shared/helper'

const addressKeyArray = getAddressKeyArray(
  {
    line1: '$addressLine1',
    city: '$city',
    state: '$state',
    zip: '$zip'
  }
)

getIngester(
  {
    debugName: 'npi-organizations',
    sourceName: 'npiOriginalProviderLocations',
    targetName: 'npiOrganizations',
    steps: [
      {
        $match: {
          Entity_Type_Code: 2,
          $or: [
            {NPI_Deactivation_Date: null},
            {NPI_Reactivation_Date: {$ne: null}}
          ]
        }
      },
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
          locationKey: {$concat: getLocationKeyArray({orgKey: '$_id', addressKeyArray})}
        }
      }
    ]
  }
)()
