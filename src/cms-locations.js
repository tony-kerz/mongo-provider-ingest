import getIngester from './shared/get-ingester'
import {getAddressKeyArray, getLocationKeyArray} from './shared/helper'

const addressKeyArray = getAddressKeyArray(
  {
    line1: '$_id.addressLine1',
    city: '$_id.city',
    state: '$_id.state',
    zip: '$_id.zip'
  }
)

getIngester(
  {
    debugName: 'cms-locations',
    sourceName: 'cmsOriginalProviderLocations',
    targetName: 'cmsLocations',
    targetIndices: [
      {locationKey: 1}
    ],
    steps: [
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
          phone: {$last: '$phone'}
        }
      },
      {
        $project: {
          _id: 0,
          locationKey: {
            $concat: getLocationKeyArray({orgKey: '$_id.orgKey', addressKeyArray})
          },
          orgKey: '$_id.orgKey',
          addressKey: {$concat: addressKeyArray},
          addressLine1: '$_id.addressLine1',
          city: '$_id.city',
          state: '$_id.state',
          zip: '$_id.zip',
          orgName: 1,
          phone: 1
        }
      }
    ]
  }
)()
