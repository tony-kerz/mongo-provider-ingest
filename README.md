# mongo-provider-ingest

node program which reads "flat" provider-location records obtained from the ["physician compare"](https://data.medicare.gov/data/physician-compare) government data set,
and normalized them in effect into three collections:

- providers
- locations
- provider-locations

normalizing data in mongo may be a little atypical, but the idea was to explore the functionality
of the [$lookup](https://docs.mongodb.com/manual/reference/operator/aggregation/lookup/) feature.

## ingest flow

the data was initially loaded into mongo using [mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/), via a command like:

```
mongoimport --verbose --db test --ignoreBlanks --type csv --file providers.csv --collection cmsProviderLocationsOriginal --drop --stopOnError --fieldFile fieldFile.dat
```

## tech notes

the code was written using current [async/await](https://github.com/tc39/ecmascript-asyncawait) syntax,
which is basically syntactic-sugar over promises that allows for a cleaner control flow by avoiding the [pyramid of doom](https://en.wikipedia.org/wiki/Pyramid_of_doom_%28programming%29)

unfortunately, there is currently an issue with promises and loops which lead to a memory-leak of sorts,
so in order to skirt that issue, the ingest program is run in segments via skip/limit (see [run.bash](./run.bash))
