import getIngester from './shared/get-ingester'

getIngester(
  {
    debugName: 'cms-organization-locations-view',
    sourceName: 'cmsProviderLocations',
    targetName: 'cmsOrganizationLocationsView',
    targetIndices: [
      {'name': 1},
      {'identifiers.extension': 1},
      {'specialties.code': 1},
      {geoPoint: '2dsphere'}
    ],
    steps: [
      {
        $lookup: {
          from: 'npiProviders',
          localField: 'npi',
          foreignField: 'npi',
          as: 'provider'
        }
      },
      {$unwind: '$provider'},
      {
        $lookup: {
          from: 'cmsLocations',
          localField: 'locationKey',
          foreignField: 'locationKey',
          as: 'location'
        }
      },
      {$unwind: '$location'},
      {$match: {'location.orgName': {$ne: null}}},
      {
        $lookup: {
          from: 'geocodedAddresses',
          localField: 'location.addressKey',
          foreignField: 'addressKey',
          as: 'geocoded'
        }
      },
      {$unwind: {path: '$geocoded', preserveNullAndEmptyArrays: true}},
      {$unwind: '$provider.specialties'},
      {
        $lookup: {
          from: 'taxonomy',
          localField: 'provider.specialties',
          foreignField: 'Code',
          as: 'taxonomy'
        }
      },
      {$unwind: '$taxonomy'},
      {
        $group: {
          _id: '$location.locationKey',
          doc: {$last: '$$ROOT'},
          practitioners: {
            $addToSet: {
              name: {
                first: '$provider.firstName',
                last: '$provider.lastName'
              }
            }
          },
          specialties: {
            $addToSet: {
              code: '$provider.specialties',
              text: '$taxonomy.Classification',
              system: {$literal: '2.16.840.1.113883.6.101'}
            }
          }
        }
      },
      {
        $project: {
          _id: 0,
          name: {$substr: ['$doc.location.orgName', 0, -1]},
          identifiers: [
            {
              authority: {$literal: 'CMS'},
              oid: {$literal: '2.16.840.1.113883.4.6'},
              extension: '$doc.location.orgKey'
            }
          ],
          practitioners: 1,
          specialties: 1,
          address: {
            line1: '$doc.location.addressLine1',
            city: '$doc.location.city',
            state: '$doc.location.state',
            zip: '$doc.location.zip'
          },
          phone: '$doc.location.phone',
          geoPoint: '$doc.geocoded.geoPoint',
          source: {$literal: 'cms'},
          orgKey: '$doc.location.orgKey',
          locationKey: '$doc.location.locationKey'
        }
      }
    ]
  }
)()
