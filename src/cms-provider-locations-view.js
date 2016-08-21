import getIngester from './shared/get-ingester'

getIngester(
  {
    debugName: 'cms-provider-locations-view',
    sourceName: 'cmsProviderLocations',
    targetName: 'cmsProviderLocationsView',
    targetIndices: [
      {'name.first': 1},
      {'name.last': 1},
      {'identifiers.extension': 1},
      {'specialties.code': 1},
      {geoPoint: '2dsphere'},
      {locationKey: 1},
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
          _id: '$_id',
          doc: {$last: '$$ROOT'},
          specialties: {
            $push: {
              code: '$provider.specialties',
              text: '$taxonomy.Classification',
              system: {$literal: '2.16.840.1.113883.6.101'}
            }
          }
        }
      },
      {
        $project: {
          name: {
            prefix: '$doc.provider.prefix',
            first: '$doc.provider.firstName',
            middle: '$doc.provider.middleName',
            last: '$doc.provider.lastName',
            suffix: '$doc.provider.suffix'
          },
          npi: '$doc.provider.npi',
          locationKey: '$doc.locationKey',
          identifiers: [
            {
              authority: {$literal: 'CMS'},
              oid: {$literal: '2.16.840.1.113883.4.6'},
              extension: '$doc.provider.npi'
            }
          ],
          specialties: 1,
          orgName: '$doc.location.orgName',
          address: {
            line1: '$doc.location.addressLine1',
            city: '$doc.location.city',
            state: '$doc.location.state',
            zip: '$doc.location.zip'
          },
          phone: '$doc.location.phone',
          geoPoint: '$doc.geocoded.geoPoint'
        }
      }
    ]
  }
)()
