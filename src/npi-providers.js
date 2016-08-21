import getIngester from './shared/get-ingester'

getIngester(
  {
    debugName: 'npi-providers',
    sourceName: 'npiOriginalProviderLocations',
    targetName: 'npiProviders',
    targetIndices: [
      {npi: 1}
    ],
    steps: [
      {
        $match: {
          $or: [
            {NPI_Deactivation_Date: null},
            {NPI_Reactivation_Date: {$ne: null}}
          ]
        }
      },
      {
        $group: {
          _id: '$NPI',
          prefix: {$last: '$Provider_Name_Prefix_Text'},
          firstName: {$last: {$ifNull: ['$Provider_First_Name','$Authorized_Official_First_Name']}},
          middleName: {$last: {$ifNull: ['$Provider_Middle_Name','$Authorized_Official_Middle_Name']}},
          lastName: {$last: {$ifNull: ['$Provider_Last_Name','$Authorized_Official_Last_Name']}},
          suffix: {$last: '$Provider_Name_Suffix_Text'},
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
          prefix: 1,
          firstName: 1,
          middleName: 1,
          lastName: 1,
          suffix: 1,
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
      }
    ]
  }
)()
