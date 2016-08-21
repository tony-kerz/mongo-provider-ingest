# mongo-provider-ingest

this repository contains a series of node programs designed to ingest and manipulate physician data
acquired from multiple government managed sources.

currently, we are using these datasets:

1. [physician-compare](https://data.medicare.gov/data/physician-compare)
1. [npiregistry.cms.hhs.gov](http://download.cms.gov/nppes/NPI_Files.html)

initial observations about the data include:

- the physician-compare data provides more information about locations that a provider works
- the npi-registry data provides better specialty information in the form of taxonomy codes

## data-flow

### initial imports

both data sets are imported into mongo collections using the [mongoimport](https://docs.mongodb.com/manual/reference/program/mongoimport/) facility.

> there is [a pending enhancement](https://jira.mongodb.org/browse/DOCS-8094) to `mongoimport` to support type specification of fields on csv imports via the `--columnsHaveTypes` flag. this is currently only available in a recent development release (`v3.3.11`), which can be downloaded locally to correctly import fields such as zip code as strings (when imported by earlier versions of `mongoimport`, the zip field has leading zeros truncated due to undesirable integer conversion).

here is the relevant documentation excerpt for reference:
```
--columnsHaveTypes                          indicated that the field list (from --fields, --fieldsFile, or --headerline) specifies
																						types; They must be in the form of '<colName>.<type>(<arg>)'. The type can be one of:
																						auto, binary, bool, date, date_go, date_ms, date_oracle, double, int32, int64, string.
																						For each of the date types, the argument is a datetime layout string. For the binary
																						type, the argument can be one of: base32, base64, hex. All other types take an empty
																						argument. Only valid for CSV and TSV imports. e.g. zipcode.string(),
																						thumbnail.binary(base64)
```

#### cmsOriginalProviderLocations
- commands
```
mongoimport --verbose --db test --ignoreBlanks --type csv --file data/cms-providers.csv --collection cmsOriginalProviderLocations --drop --stopOnError --fieldFile data/physician-compare-fields.dat --columnsHaveTypes --parseGrace=autoCast
```
remove record created from header line
```
mongo localhost:27017/test --eval 'db.cmsOriginalProviderLocations.remove({npi: "NPI"})'
```
- sample
```
> db.cmsOriginalProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("57b67ef7454ead5d7fefa3f1"),
	"npi" : "1245208719",
	"pac" : "0547333676",
	"pei" : "I20080722000571",
	"lastName" : "SPARROW",
	"firstName" : "JOHN",
	"gender" : "M",
	"school" : "UNIVERSITY OF MISSISSIPPI SCHOOL OF MEDICINE",
	"gradYear" : 1982,
	"specialty" : "PLASTIC AND RECONSTRUCTIVE SURGERY",
	"orgName" : "JACKSON CLINIC PA",
	"groupPac" : "2668376872",
	"groupMemberCount" : 227,
	"addressLine1" : "87 MURRAY GUARD DR",
	"addressLine2" : "B",
	"city" : "JACKSON",
	"state" : "TN",
	"zip" : "383053775",
	"phone" : "7316641375",
	"hospitalCcn1" : "440002",
	"hospitalLbn1" : "JACKSON MADISON COUNTY GENERAL HOSPITAL",
	"medicareFlag" : "Y",
	"measuresFlag" : "Y"
}
```

#### npiOriginalProviderLocations

- commands
```
mongoimport --verbose --db test --ignoreBlanks --type csv --file npi-providers.csv --collection npiOriginalProviderLocations --drop --stopOnError --fieldFile data/npi-fields.dat --columnsHaveTypes --parseGrace=autoCast
```
remove record created from header line
```
mongo localhost:27017/test --eval 'db.npiOriginalProviderLocations.remove({NPI: "NPI"})'
```

- sample
```
> db.npiOriginalProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("57b687f1454ead5d7f0f655d"),
	"NPI" : "1679576722",
	"Entity_Type_Code" : 1,
	"Provider_Last_Name" : "WIEBE",
	"Provider_First_Name" : "DAVID",
	"Provider_Middle_Name" : "A",
	"Provider_Credential_Text" : "M.D.",
	"Provider_First_Line_Business_Mailing_Address" : "PO BOX 2168",
	"Provider_Business_Mailing_Address_City_Name" : "KEARNEY",
	"Provider_Business_Mailing_Address_State_Name" : "NE",
	"Provider_Business_Mailing_Address_Postal_Code" : "688482168",
	"Provider_Business_Mailing_Address_Country_Code" : "US",
	"Provider_Business_Mailing_Address_Telephone_Number" : "3088652512",
	"Provider_Business_Mailing_Address_Fax_Number" : "3088652506",
	"Provider_First_Line_Business_Practice_Location_Address" : "3500 CENTRAL AVE",
	"Provider_Business_Practice_Location_Address_City_Name" : "KEARNEY",
	"Provider_Business_Practice_Location_Address_State_Name" : "NE",
	"Provider_Business_Practice_Location_Address_Postal_Code" : "688472944",
	"Provider_Business_Practice_Location_Address_Country_Code" : "US",
	"Provider_Business_Practice_Location_Address_Telephone_Number" : "3088652512",
	"Provider_Business_Practice_Location_Address_Fax_Number" : "3088652506",
	"Provider_Enumeration_Date" : "05/23/2005",
	"Last_Update_Date" : "07/08/2007",
	"Provider_Gender_Code" : "M",
	"Healthcare_Provider_Taxonomy_Code_1" : "207X00000X",
	"Provider_License_Number_1" : "12637",
	"Provider_License_Number_State_Code_1" : "NE",
	"Healthcare_Provider_Primary_Taxonomy_Switch_1" : "Y",
	"Other_Provider_Identifier_1" : "46969",
	"Other_Provider_Identifier_Type_Code_1" : 1,
	"Other_Provider_Identifier_State_1" : "KS",
	"Other_Provider_Identifier_Issuer_1" : "BCBS",
	"Other_Provider_Identifier_2" : "645540",
	"Other_Provider_Identifier_Type_Code_2" : 1,
	"Other_Provider_Identifier_State_2" : "KS",
	"Other_Provider_Identifier_Issuer_2" : "FIRSTGUARD",
	"Other_Provider_Identifier_3" : "B67599",
	"Other_Provider_Identifier_Type_Code_3" : 2,
	"Other_Provider_Identifier_4" : "1553",
	"Other_Provider_Identifier_Type_Code_4" : 1,
	"Other_Provider_Identifier_State_4" : "NE",
	"Other_Provider_Identifier_Issuer_4" : "BCBS",
	"Other_Provider_Identifier_5" : "046969WI",
	"Other_Provider_Identifier_Type_Code_5" : 4,
	"Other_Provider_Identifier_State_5" : "KS",
	"Other_Provider_Identifier_6" : "93420WI",
	"Other_Provider_Identifier_Type_Code_6" : 4,
	"Other_Provider_Identifier_State_6" : "NE",
	"Is_Sole_Proprietor" : "X"
}
>
```
### normalization steps
the strategy employed here is to create normalized collections to facilitate data management and to then generate denormalized collections to accommodate specific query use-cases.

these denormalized collections can be compared to the [materialized-view](https://en.wikipedia.org/wiki/Materialized_view) concept found in other database products.

sections below describe normalized entities

#### normalize providers

- example cli: `npm run npi-providers`
- [code](src/npi-providers.js)
- sample
```
> db.npiProviders.find().limit(1).pretty()
{
	"_id" : ObjectId("57b7072abfd58db7f9795eea"),
	"prefix" : null,
	"firstName" : "GERARDO",
	"middleName" : null,
	"lastName" : "GOMEZ",
	"suffix" : null,
	"npi" : "1003000100",
	"specialties" : [
		"171M00000X"
	]
}
```

#### normalize (cms) locations
- example cli: `npm run cms-locations`
- [code](src/cms-locations.js)
- sample
```
> db.cmsLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("57b6f028454ead5d7f5a8675"),
	"orgName" : "FAMILY PRACTICE ASSOCIATES OF ULYSSES LLC",
	"phone" : "6203565870",
	"locationKey" : "3779523329:202 W KANSAS AVE:ULYSSES:KS:678802034",
	"orgKey" : "3779523329",
	"addressKey" : "202 W KANSAS AVE:ULYSSES:KS:678802034",
	"addressLine1" : "202 W KANSAS AVE",
	"city" : "ULYSSES",
	"state" : "KS",
	"zip" : "678802034"
}
```
> cms data can have multiple location records for a single practice,
> so we call this collection `cmsLocations` v something like `cmsOrganizations`

#### normalize (cms) provider-locations
- example cli: `npm run cms-provider-locations`
- [code](src/cms-provider-locations.js)
- sample
```
> db.cmsProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("57b6f11d454ead5d7f5f8924"),
	"npi" : "1003000126",
	"locationKey" : "4587979323:1900 ELECTRIC RD:SALEM:VA:241537474"
}
```

#### normalize (npi) organizations
- example cli: `npm run npi-organizations`
- [code](src/npi-organizations.js)
- sample
```
> db.npiOrganizations.find().limit(1).pretty()
{
	"_id" : ObjectId("57b6f428454ead5d7f88596a"),
	"name" : "STEVEN ENGEL PEDIATRICS",
	"addressLine1" : "1700 NEUSE BLVD",
	"city" : "NEW BERN",
	"state" : "NC",
	"zip" : "285602304",
	"phone" : "2526373799",
	"npi" : "1003000118",
	"specialties" : [
		"208000000X"
	],
	"addressKey" : "1700 NEUSE BLVD:NEW BERN:NC:285602304",
	"locationKey" : "1003000118:1700 NEUSE BLVD:NEW BERN:NC:285602304"
}
```
> npi data only has a single record for an organization with a single location,
> so we call this collection `npiOrganizations` v something like `npiLocations`

#### geocode addresses
- example cli: `npm run geocode -- --query='{"state": "NY"}' --sourceCollection=npiOrganizations`
- [code](src/geocoder.js)
- sample
```
> db.geocodedAddresses.find().limit(1).pretty()
{
	"_id" : ObjectId("5783db9adfd7e6b6bfef8356"),
	"geoPoint" : {
		"type" : "Point",
		"coordinates" : [
			-73.35333,
			42.855021
		]
	},
	"addressLine1" : "75 PINE VALLEY RD",
	"city" : "HOOSICK FALLS",
	"state" : "NY",
	"zip" : 120903808,
	"addressKey" : "75 PINE VALLEY RD:HOOSICK FALLS:NY:120903808"
}
```

> the `geocoded-addresses` collection is normalized with the intent to persist indefinitely to minimize the frequency of ~300ms callouts to an external rate-limited geocoding service.
<!-- -->
> geocode program currently defaults `sourceCollection` to `cmsLocations`, but this can be overridden via the `--sourceCollection` parameter to geocode `npiOrganizations` (which is currently being used for the `npiOrganizationLocationsView`)
<!-- -->
> a max of 30K records will be geocoded in one run by default per rate limits, but this can be overridden like so:
```
npm run geocode -- --limit=40000
```

#### geocode zips

since the ui is currently only setup to filter based on distance from a zipcode (vs a more detailed address), we are capitalizing on that fact by maintaining a list of zip to lat/lon values.

we are currently sourcing that data from [here](https://boutell.com/zipcodes/).

- commands
```
~/m/m/bin $ ./mongoimport --verbose --db=test --collection=geozip --stopOnError --file=/Users/tony/git/mongo-provider-ingest/data/zipcode.csv --type csv --headerline --columnsHaveTypes --drop --ignoreBlanks --parseGrace=autoCast --fieldFile data/geozip-fields.dat
```
remove record created from header line
```
mongo localhost:27017/test --eval 'db.geozip.remove({zip: "zip"})'
```
- sample
```
> db.geozip.find().limit(1).pretty()
{
	"_id" : ObjectId("57b31b7f454ead5d7feefb3a"),
	"zip" : "00210",
	"city" : "Portsmouth",
	"state" : "NH",
	"latitude" : 43.005895,
	"longitude" : -71.013202,
	"timezone" : -5,
	"dst" : 1
}
```

### denormalization steps

#### denormalize provider-locations-view
- example cli: `npm run provider-locations-view -- --query='{"location.state": "NY"}'`
- [code](src/provider-locations-view.js)
- sample
```
> db.cmsProviderLocationsView.find().limit(1).pretty()
{
	"_id" : ObjectId("57b6f11d454ead5d7f5f894d"),
	"specialties" : [
		{
			"code" : "208100000X",
			"text" : "Physical Medicine & Rehabilitation",
			"system" : "2.16.840.1.113883.6.101"
		}
	],
	"name" : {
		"prefix" : null,
		"first" : "DAVID",
		"middle" : "M",
		"last" : "KANTER",
		"suffix" : null
	},
	"npi" : "1003001371",
	"locationKey" : "8123911500:750 E ADAMS ST:SYRACUSE:NY:132102342",
	"identifiers" : [
		{
			"authority" : "CMS",
			"oid" : "2.16.840.1.113883.4.6",
			"extension" : "1003001371"
		}
	],
	"orgName" : "PHYSICAL MEDICINE AND REHAB MEDICAL SERVICE GROUP",
	"address" : {
		"line1" : "750 E ADAMS ST",
		"city" : "SYRACUSE",
		"state" : "NY",
		"zip" : "132102342"
	},
	"phone" : "3154645820",
	"geoPoint" : {
		"type" : "Point",
		"coordinates" : [
			-76.139072,
			43.042291
		]
	}
}
```

#### denormalize (npi) organization-locations-view
- example cli: `npm run npi-organization-locations-view -- --query='{"city": "NEW YORK"}'`
- [code](src/npi-organization-locations-view.js)
- sample
```
> db.npiOrganizationLocationsView.find({'address.state': 'NY'}).limit(1).pretty()
{
	"_id" : ObjectId("57b6f428454ead5d7f88597f"),
	"specialties" : [
		{
			"code" : "261QP2300X",
			"text" : "Clinic/Center",
			"system" : "2.16.840.1.113883.6.101"
		}
	],
	"name" : "MMC MEDICAL PARK AT 1635 POPLAR",
	"identifiers" : [
		{
			"authority" : "CMS",
			"oid" : "2.16.840.1.113883.4.6",
			"extension" : "1003000779"
		}
	],
	"address" : {
		"line1" : "MMC MEDICAL PARK AT 1635 POPLAR",
		"city" : "BRONX",
		"state" : "NY",
		"zip" : "104612659"
	},
	"phone" : "9143774722",
	"geoPoint" : {
		"type" : "Point",
		"coordinates" : [
			-73.87541,
			40.85677
		]
	},
	"source" : "npi",
	"npi" : "1003000779",
	"locationKey" : "1003000779:MMC MEDICAL PARK AT 1635 POPLAR:BRONX:NY:104612659"
}
```
#### denormalize (cms) organization-locations-view
- example cli: `npm run cms-organization-locations-view -- --query='{"city": "NEW YORK"}'`
- [code](src/cms-organization-locations-view.js)
- sample
```
> db.cmsOrganizationLocationsView.find({'address.state': 'NY'}).limit(1).pretty()
{
	"_id" : ObjectId("57b8e4abbfd58db7f9cab434"),
	"practitioners" : [
		{
			"name" : {
				"first" : "DIMA",
				"last" : "TEITELMAN"
			}
		}
	],
	"specialties" : [
		{
			"code" : "207RC0000X",
			"text" : "Internal Medicine",
			"system" : "2.16.840.1.113883.6.101"
		}
	],
	"name" : "PRIMARY CARE ASSOCIATES, LLP",
	"identifiers" : [
		{
			"authority" : "CMS",
			"oid" : "2.16.840.1.113883.4.6",
			"extension" : "0042100257"
		}
	],
	"address" : {
		"line1" : "15403 10TH AVE",
		"city" : "WHITESTONE",
		"state" : "NY",
		"zip" : "113571912"
	},
	"phone" : "7187679400",
	"geoPoint" : {
		"type" : "Point",
		"coordinates" : [
			-73.99931,
			40.638252
		]
	},
	"source" : "cms",
	"orgKey" : "0042100257",
	"locationKey" : "0042100257:15403 10TH AVE:WHITESTONE:NY:113571912"
}
```

#### merge cms and npi organization-locations-view
currently the cms data is smaller so it is more efficient to export/import that into the npi data
```
mongoexport --db test --collection cmsOrganizationLocationsView --out colv.json
```
```
mongoimport --verbose --db=test --collection=npiOrganizationLocationsView --stopOnError --file=colv.json --upsertFields=locationKey
```
```
mongo localhost:27017/test --eval 'db.organizationLocationsView.drop()'
mongo localhost:27017/test --eval 'db.npiOrganizationLocationsView.renameCollection("organizationLocationsView")'
```
> rename `npiOrganizationLocationsView` to just `organizationLocationsView` after we add cms data to it

### other notes

##### export/import
```
rm -rf dump
mongodump  --db test --collection organizationLocationsView --gzip
tar czf organizationLocationsView.tar.gz dump
```
> apparently, if the `--archive` flag to `mongodump` is used on mac, the resultant archive doesn't `mongorestore` on windows, so we omit it here
