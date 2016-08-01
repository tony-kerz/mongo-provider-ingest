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

#### cmsOriginalProviderLocations

- command:
```
mongoimport --verbose --db test --ignoreBlanks --type csv --file cms-providers.csv --collection cmsOriginalProviderLocations --drop --stopOnError --fieldFile data/physician-compare-field-file.dat
```
- sample:
```
> db.cmsOriginalProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("57773a1acb8b73b4d188e764"),
	"npi" : 1841433687,
	"pac" : NumberLong("4587815725"),
	"pei" : "I20121115000354",
	"lastName" : "LOPEZ",
	"firstName" : "LUIS",
	"gender" : "M",
	"school" : "OTHER",
	"gradYear" : 2005,
	"specialty" : "INTERNAL MEDICINE",
	"orgName" : "LEHIGH VALLEY PHYSICIAN GROUP",
	"groupPac" : NumberLong("3072425123"),
	"groupMemberCount" : 867,
	"addressLine1" : "1259 S CEDAR CREST BLVD",
	"addressLine2" : "230 LVPG ADULT AND PEDIATRIC PSYCHIATRY",
	"city" : "ALLENTOWN",
	"state" : "PA",
	"zip" : 181036376,
	"phone" : NumberLong("6104025900"),
	"hospitalCcn1" : 390133,
	"hospitalLbn1" : "LEHIGH VALLEY HOSPITAL",
	"hospitalCcn2" : 390263,
	"hospitalLbn2" : "LEHIGH VALLEY HOSPITAL MUHLENBERG",
	"medicareFlag" : "Y",
	"measuresFlag" : "Y",
	"ehrFlag" : "Y"
}
```

#### npiOriginalProviderLocations

- command:

```
mongoimport --verbose --db test --ignoreBlanks --type csv --file npi-providers.csv --collection npiOriginalProviderLocations --drop --stopOnError --fieldFile data/npi-field-file.dat
```

- sample:
```
> db.npiOriginalProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("5772abca20782221ef8aadc9"),
	"NPI" : 1679576722,
	"Entity_Type_Code" : 1,
	"Provider_Last_Name" : "WIEBE",
	"Provider_First_Name" : "DAVID",
	"Provider_Middle_Name" : "A",
	"Provider_Credential_Text" : "M.D.",
	"Provider_First_Line_Business_Mailing_Address" : "PO BOX 2168",
	"Provider_Business_Mailing_Address_City_Name" : "KEARNEY",
	"Provider_Business_Mailing_Address_State_Name" : "NE",
	"Provider_Business_Mailing_Address_Postal_Code" : 688482168,
	"Provider_Business_Mailing_Address_Country_Code" : "US",
	"Provider_Business_Mailing_Address_Telephone_Number" : NumberLong("3088652512"),
	"Provider_Business_Mailing_Address_Fax_Number" : NumberLong("3088652506"),
	"Provider_First_Line_Business_Practice_Location_Address" : "3500 CENTRAL AVE",
	"Provider_Business_Practice_Location_Address_City_Name" : "KEARNEY",
	"Provider_Business_Practice_Location_Address_State_Name" : "NE",
	"Provider_Business_Practice_Location_Address_Postal_Code" : 688472944,
	"Provider_Business_Practice_Location_Address_Country_Code" : "US",
	"Provider_Business_Practice_Location_Address_Telephone_Number" : NumberLong("3088652512"),
	"Provider_Business_Practice_Location_Address_Fax_Number" : NumberLong("3088652506"),
	"Provider_Enumeration_Date" : "05/23/2005",
	"Last_Update_Date" : "07/08/2007",
	"Provider_Gender_Code" : "M",
	"Healthcare_Provider_Taxonomy_Code_1" : "207X00000X",
	"Provider_License_Number_1" : 12637,
	"Provider_License_Number_State_Code_1" : "NE",
	"Healthcare_Provider_Primary_Taxonomy_Switch_1" : "Y",
	"Other_Provider_Identifier_1" : 46969,
	"Other_Provider_Identifier_Type_Code_1" : 1,
	"Other_Provider_Identifier_State_1" : "KS",
	"Other_Provider_Identifier_Issuer_1" : "BCBS",
	"Other_Provider_Identifier_2" : 645540,
	"Other_Provider_Identifier_Type_Code_2" : 1,
	"Other_Provider_Identifier_State_2" : "KS",
	"Other_Provider_Identifier_Issuer_2" : "FIRSTGUARD",
	"Other_Provider_Identifier_3" : "B67599",
	"Other_Provider_Identifier_Type_Code_3" : 2,
	"Other_Provider_Identifier_4" : 1553,
	"Other_Provider_Identifier_Type_Code_4" : 1,
	"Other_Provider_Identifier_State_4" : "NE",
	"Other_Provider_Identifier_Issuer_4" : "BCBS",
	"Other_Provider_Identifier_5" : "046969WI",
	"Other_Provider_Identifier_Type_Code_5" : 4,
	"Other_Provider_Identifier_State_5" : "KS",
	"Other_Provider_Identifier_6" : "93420WI",
	"Other_Provider_Identifier_Type_Code_6" : 4,
	"Other_Provider_Identifier_State_6" : "NE",
	"Is_Sole_Proprietor" : "X",
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
- example result:
```
> db.npiProviders.find().limit(1).pretty()
{
	"_id" : ObjectId("5780f91edfd7e6b6bf701080"),
	"firstName" : "GERARDO",
	"middleName" : null,
	"lastName" : "GOMEZ",
	"npi" : 1003000100,
	"specialties" : [
		"171M00000X"
	]
}
```

#### normalize (cms) locations
- example cli: `npm run cms-locations`
- [code](src/cms-locations.js)
- example result:
```
> db.cmsLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("57828252dfd7e6b6bfea80a6"),
	"orgName" : "FAMILY PRACTICE ASSOCIATES OF ULYSSES LLC",
	"phone" : "6203565870",
	"locationKey" : "3779523329:202 W KANSAS AVE:ULYSSES:KS:678802034",
	"addressKey" : "202 W KANSAS AVE:ULYSSES:KS:678802034",
	"addressLine1" : "202 W KANSAS AVE",
	"city" : "ULYSSES",
	"state" : "KS",
	"zip" : 678802034
}
```
> cms data can have multiple location records for a single practice,
> so we call this collection `cmsLocations` v something like `cmsOrganizations`

#### normalize (cms) provider-locations
- example cli: `npm run cms-provider-locations`
- [code](src/cms-provider-locations.js)
- example result:
```
> db.cmsProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("5783ed5cdfd7e6b6bfefd2f9"),
	"npi" : 1003000126,
	"locationKey" : "4587979323:1900 ELECTRIC RD:SALEM:VA:241537474"
}
```

#### normalize (npi) organizations
- example cli: `npm run npi-organizations`
- [code](src/npi-organizations.js)
- example result:
```
> db.npiOrganizations.find().limit(1).pretty()
{
	"_id" : ObjectId("57914f761604c70ac2f48faa"),
	"name" : "STEVEN ENGEL PEDIATRICS",
	"addressLine1" : "1700 NEUSE BLVD",
	"city" : "NEW BERN",
	"state" : "NC",
	"zip" : 285602304,
	"phone" : NumberLong("2526373799"),
	"npi" : 1003000118,
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
- example result:
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

### denormalization steps

#### denormalize provider-locations-view
- example cli: `npm run provider-locations-view -- --query='{"location.state": "NY"}'`
- [code](src/provider-locations-view.js)
- example result:
```
> db.cmsProviderLocationsView.find({'address.state': 'NY'}).limit(1).pretty()
{
	"_id" : ObjectId("5783ed5cdfd7e6b6bfefd322"),
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
	"identifiers" : [
		{
			"authority" : "CMS",
			"oid" : "2.16.840.1.113883.4.6",
			"extension" : 1003001371
		}
	],
	"orgName" : "PHYSICAL MEDICINE AND REHAB MEDICAL SERVICE GROUP",
	"address" : {
		"line1" : "750 E ADAMS ST",
		"city" : "SYRACUSE",
		"state" : "NY",
		"zip" : 132102342
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
- sample:
```
> db.npiOrganizationLocationsView.find().limit(1).pretty()
{
	"_id" : ObjectId("57914f8c1604c70ac2061dca"),
	"specialties" : [
		{
			"code" : "207VE0102X",
			"text" : "Obstetrics & Gynecology",
			"system" : "2.16.840.1.113883.6.101"
		},
		{
			"code" : "207VG0400X",
			"text" : "Obstetrics & Gynecology",
			"system" : "2.16.840.1.113883.6.101"
		}
	],
	"name" : "REPRODUCTIVE MEDICINE ASSOCIATES OF BROOKLYN LLP",
	"identifiers" : [
		{
			"authority" : "CMS",
			"oid" : "2.16.840.1.113883.4.6",
			"extension" : 1992999619
		}
	],
	"address" : {
		"line1" : "225 BROADWAY",
		"city" : "NEW YORK",
		"state" : "NY",
		"zip" : 100073001
	},
	"phone" : 2127667272,
	"geoPoint" : {
		"type" : "Point",
		"coordinates" : [
			-75.497811,
			44.691669
		]
	}
}
```
#### denormalize (cms) organization-locations-view
- example cli: `npm run cms-organization-locations-view -- --query='{"city": "NEW YORK"}'`
- [code](src/cms-organization-locations-view.js)
- sample:
```
> db.cmsOrganizationLocationsView.find({'geoPoint': {$ne: null}}).limit(1).pretty()
{
	"_id" : ObjectId("579ae16795d9d8ce9b8a1754"),
	"practitioners" : [
		{
			"name" : {
				"first" : "PETER",
				"last" : "BREINGAN"
			}
		},
		{
			"name" : {
				"first" : "LAUREN",
				"last" : "SMITH"
			}
		},
		{
			"name" : {
				"first" : "RICHARD",
				"last" : "DELUCA"
			}
		}
	],
	"specialties" : [
		{
			"code" : "152W00000X",
			"text" : "Optometrist",
			"system" : "2.16.840.1.113883.6.101"
		},
		{
			"code" : "207W00000X",
			"text" : "Ophthalmology",
			"system" : "2.16.840.1.113883.6.101"
		}
	],
	"name" : "PETER J. BREINGAN MD AND RICHARD L. DELUCA MD",
	"identifiers" : [
		{
			"authority" : "CMS",
			"oid" : "2.16.840.1.113883.4.6",
			"extension" : "1052200805"
		}
	],
	"address" : {
		"line1" : "132 E 76TH ST",
		"city" : "NEW YORK",
		"state" : "NY",
		"zip" : 100212850
	},
	"phone" : "2125052151",
	"geoPoint" : {
		"type" : "Point",
		"coordinates" : [
			-73.962167,
			40.773891
		]
	},
	"source" : "cms"
}
```

#### merge cms and npi organization-locations-view
currently the cms data is smaller so it is more efficient to export/import that into the npi data
```
mongoexport --db test --collection cmsOrganizationLocationsView --out colv.json
```
```
mongoimport --verbose --db=test --collection=npiOrganizationLocationsView --stopOnError --file=colv.json --upsertFields=source,identifiers.extension
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
