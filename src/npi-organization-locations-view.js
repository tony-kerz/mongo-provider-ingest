import getIngester from './shared/get-ingester'

getIngester(
  {
    debugName: 'organization-locations-view',
    sourceName: 'npiOrganizations',
    targetName: 'npiOrganizationLocationsView',
    targetIndices: [
      {'name': 1},
      {'identifiers.extension': 1},
      {'specialties.code': 1},
      {geoPoint: '2dsphere'},
      {locationKey: 1}
    ],
    steps: [
      {
        $lookup: {
          from: 'geocodedAddresses',
          localField: 'addressKey',
          foreignField: 'addressKey',
          as: 'geocoded'
        }
      },
      {$unwind: {path: '$geocoded', preserveNullAndEmptyArrays: true}},
      {$unwind: '$specialties'},
      {
        $lookup: {
          from: 'taxonomy',
          localField: 'specialties',
          foreignField: 'Code',
          as: 'taxonomy'
        }
      },
      {$unwind: '$taxonomy'},
      {
        $group: {
          _id: '$_id',
          doc: {$last: '$$ROOT'},
          specialties: {
            $push: {
              code: '$specialties',
              text: '$taxonomy.Classification',
              system: {$literal: '2.16.840.1.113883.6.101'}
            }
          }
        }
      },
      {
        $project: {
          name: '$doc.name',
          identifiers: [
            {
              authority: {$literal: 'CMS'},
              oid: {$literal: '2.16.840.1.113883.4.6'},
              extension: '$doc.npi'
            }
          ],
          specialties: 1,
          address: {
            line1: '$doc.addressLine1',
            city: '$doc.city',
            state: '$doc.state',
            zip: '$doc.zip'
          },
          phone: '$doc.phone',
          geoPoint: '$doc.geocoded.geoPoint',
          source: {$literal: 'npi'},
          npi: '$doc.npi',
          locationKey: '$doc.locationKey'
        }
      }
    ]
  }
)()
