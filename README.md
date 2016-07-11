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

#### providers

- program: [`npi-providers.js`](src/npi-providers.js)
- sample:
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

#### locations

- program: [`cms-providers.js`](src/cms-providers.js)
- sample:
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

#### provider-locations
- program: [`cms-provider-locations.js`](src/cms-provider-locations.js)
- sample:
```
> db.cmsProviderLocations.find().limit(1).pretty()
{
	"_id" : ObjectId("5783ed5cdfd7e6b6bfefd2f9"),
	"npi" : 1003000126,
	"locationKey" : "4587979323:1900 ELECTRIC RD:SALEM:VA:241537474"
}
```

#### geocoded-addresses
- program: [`geocoder.js`](src/geocoder.js)
- sample:
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

> the `geocoded-addresses` collection is normalized to facilitate the ongoing geocoding process which is relatively expensive because of it's requirement to call an external rate-limited geocoding service

### denormalization steps

#### provider-locations
- program: [`denorm.js`](src/denorm.js)
- sample:
```
> db.cmsDenormedProviderLocations.find({geoPoint: {$ne: null}}).limit(1).pretty()
{
	"_id" : ObjectId("5783ed5cdfd7e6b6bfefd322"),
	"npi" : 1003001371,
	"firstName" : "DAVID",
	"middleName" : "M",
	"lastName" : "KANTER",
	"specialties" : [
		"208100000X"
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
