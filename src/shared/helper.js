import minimist from 'minimist'
import mongodb from 'mongodb'

const SEPARATOR = ':'

export function getLocationKeyArray({orgKey, addressKeyArray}) {
  return [
    orgKey,
    SEPARATOR
  ]
  .concat(addressKeyArray)
}

export function getAddressKeyArray({line1, city, state, zip}) {
  return [
    line1,
    SEPARATOR,
    city,
    SEPARATOR,
    state,
    SEPARATOR,
    zip
  ]
}

export async function connect(url, socketTimeoutSeconds=0) {
  return await mongodb.MongoClient.connect(
    url,
    {
      server: {
        socketOptions: {
          socketTimeoutMS: socketTimeoutSeconds * 1000
        }
      }
    }
  )
}

export function jsonArg(name, dflt={}){
  const argv = minimist(process.argv.slice(2))
  return argv[name] ? JSON.parse(argv[name]) : dflt
}
