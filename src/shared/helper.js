const SEPARATOR = ':'

export function getLocationKeyArray(orgKey, addressKeyArray) {
  return [
    {$substr: [orgKey, 0, -1]},
    SEPARATOR
  ]
  .concat(addressKeyArray)
}

export function getAddressKeyArray(line1, city, state, zip) {
  return [
    {$substr: [line1, 0, -1]},
    SEPARATOR,
    {$substr: [city, 0, -1]},
    SEPARATOR,
    {$substr: [state, 0, -1]},
    SEPARATOR,
    {$substr: [zip, 0, -1]}
  ]
}
