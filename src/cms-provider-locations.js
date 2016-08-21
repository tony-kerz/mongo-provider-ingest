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
    debugName: 'cms-provider-locations',
    sourceName: 'cmsOriginalProviderLocations',
    targetName: 'cmsProviderLocations',
    steps: [
      {
        $group: {
          _id: {
            npi: '$npi',
            orgKey: {$ifNull: ['$groupPac', '$npi']},
            addressLine1: '$addressLine1',
            city: '$city',
            state: '$state',
            zip: '$zip'
          },
        }
      },
      {
        $project: {
          _id: 0,
          npi: '$_id.npi',
          locationKey: {
            $concat: getLocationKeyArray({orgKey: '$_id.orgKey', addressKeyArray})
          },
          orgKey: '$_id.orgKey',
          address: {
            line1: '$_id.addressLine1',
            city: '$_id.city',
            state: '$_id.state',
            zip: '$_id.zip'
          }
        }
      }
    ]
  }
)()
