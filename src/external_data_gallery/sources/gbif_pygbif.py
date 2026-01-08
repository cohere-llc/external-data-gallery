"""
Prompts and examples for GBIF data using pygbif
"""

import pygbif
from typing import Any, Dict


def schema() -> Dict[str, Any]:
    """Returns the schema for the GBIF dataset via pygbif."""
    fields = {
        "occurrence.taxonKey": "[int] A GBIF occurrence identifier",
        "occurrence.oq": "[str] Simple search parameter. The value for this parameter can be a simple word or a phrase.",
        "occurrence.spellCheck": "[bool] If True ask GBIF to check your spelling of the value passed to the search parameter. IMPORTANT: This only checks the input to the search parameter, and no others. Default: False",
        "occurrence.repatriated": "[str] Searches for records whose publishing country is different to the country where the record was recorded in",
        "occurrence.kingdomKey": "[int] Kingdom classification key",
        "occurrence.phylumKey": "[int] Phylum classification key",
        "occurrence.classKey": "[int] Class classification key",
        "occurrence.orderKey": "[int] Order classification key",
        "occurrence.familyKey": "[int] Family classification key",
        "occurrence.genusKey": "[int] Genus classification key",
        "occurrence.subgenusKey": "[int] Subgenus classification key",
        "occurrence.scientificName": "[str] A scientific name from the GBIF backbone. All included and synonym taxa are included in the search.",
        "occurrence.datasetKey": "[str] The occurrence dataset key (a uuid)",
        "occurrence.catalogNumber": "[str] An identifier of any form assigned by the source within a physical collection or digital dataset for the record which may not unique, but should be fairly unique in combination with the institution and collection code.",
        "occurrence.recordedBy": "[str] The person who recorded the occurrence.",
        "occurrence.recordedByID": "[str] Identifier (e.g. ORCID) for the person who recorded the occurrence",
        "occurrence.identifiedByID": "[str] Identifier (e.g. ORCID) for the person who provided the taxonomic identification of the occurrence.",
        "occurrence.collectionCode": "[str] An identifier of any form assigned by the source to identify the physical collection or digital dataset uniquely within the text of an institution.",
        "occurrence.institutionCode": "[str] An identifier of any form assigned by the source to identify the institution the record belongs to. Not guaranteed to be unique.",
        "occurrence.country": "[str] The 2-letter country code (as per ISO-3166-1) of the country in which the occurrence was recorded. See here http://en.wikipedia.org/wiki/ISO_3166-1_alpha-2",
        "occurrence.basisOfRecord": "[str] Basis of record, as defined in our BasisOfRecord enum here http://gbif.github.io/gbif-api/apidocs/org/gbif/api/vocabulary/BasisOfRecord.html Acceptable values are:",
        "occurrence.basisOfRecord.FOSSIL_SPECIMEN": "[str] An occurrence record describing a fossilized specimen.",
        "occurrence.basisOfRecord.HUMAN_OBSERVATION": "[str] An occurrence record describing an observation made by one or more people.",
        "occurrence.basisOfRecord.LIVING_SPECIMEN": "[str] An occurrence record describing a living specimen.",
        "occurrence.basisOfRecord.MACHINE_OBSERVATION": "[str] An occurrence record describing an observation made by a machine.",
        "occurrence.basisOfRecord.MATERIAL_CITATION": "[str] An occurrence record based on a reference to a scholarly publication.",
        "occurrence.basisOfRecord.OBSERVATION": "[str] An occurrence record describing an observation.",
        "occurrence.basisOfRecord.OCCURRENCE": "[str] An existence of an organism at a particular place and time. No more specific basis.",
        "occurrence.basisOfRecord.PRESERVED_SPECIMEN": "[str] An occurrence record describing a preserved specimen.",
        "occurrence.eventDate": "[date] Occurrence date in ISO 8601 format: yyyy, yyyy-MM, yyyy-MM-dd, or MM-dd. Supports range queries, smaller,larger (e.g., 1990,1991, whereas 1991,1990 wouldn’t work)",
        "occurrence.year": "[int] The 4 digit year. A year of 98 will be interpreted as AD 98. Supports range queries, smaller,larger (e.g., 1990,1991, whereas 1991,1990 wouldn’t work)",
        "occurrence.month": "[int] The month of the year, starting with 1 for January. Supports range queries, smaller,larger (e.g., 1,2, whereas 2,1 wouldn’t work)",
        "occurrence.decimalLatitude": "[float] Latitude in decimals between -90 and 90 based on WGS 84. Supports range queries, smaller,larger (e.g., 25,30, whereas 30,25 wouldn’t work)",
        "occurrence.decimalLongitude": "[float] Longitude in decimals between -180 and 180 based on WGS 84. Supports range queries (e.g., -0.4,-0.2, whereas -0.2,-0.4 wouldn’t work).",
        "occurrence.publishingCountry": "[str] The 2-letter country code (as per ISO-3166-1) of the country in which the occurrence was recorded.",
        "occurrence.elevation": "[int/str] Elevation in meters above sea level. Supports range queries, smaller,larger (e.g., 5,30, whereas 30,5 wouldn’t work)",
        "occurrence.depth": "[int/str] Depth in meters relative to elevation. For example 10 meters below a lake surface with given elevation. Supports range queries, smaller,larger (e.g., 5,30, whereas 30,5 wouldn’t work)",
        "occurrence.geometry": "[str] Searches for occurrences inside a polygon described in Well Known Text (WKT) format. A WKT shape written as either POINT, LINESTRING, LINEARRING POLYGON, or MULTIPOLYGON. Example of a polygon: ((30.1 10.1, 20, 20 40, 40 40, 30.1 10.1)) would be queried as http://bit.ly/1BzNwDq. Polygons must have counter-clockwise ordering of points.",
        "occurrence.hasGeospatialIssue": "[bool] Includes/excludes occurrence records which contain spatial issues (as determined in our record interpretation), i.e. hasGeospatialIssue=TRUE returns only those records with spatial issues while hasGeospatialIssue=FALSE includes only records without spatial issues. The absence of this parameter returns any record with or without spatial issues.",
        "occurrence.issue": "[str] One or more of many possible issues with each occurrence record. See Details. Issues passed to this parameter filter results by the issue.",
        "occurrence.hasCoordinate": "[bool] Return only occurence records with lat/long data (True) or all records (False, default).",
        "occurrence.typeStatus": "[str] Type status of the specimen. One of many options. See ?typestatus",
        "occurrence.recordNumber": "[int] Number recorded by collector of the data, different from GBIF record number. See http://rs.tdwg.org/dwc/terms/#recordNumber} for more info",
        "occurrence.lastInterpreted": "[date] Date the record was last modified in GBIF, in ISO 8601 format: yyyy, yyyy-MM, yyyy-MM-dd, or MM-dd. Supports range queries, smaller,larger (e.g., 1990,1991, whereas 1991,1990 wouldn’t work)",
        "occurrence.continent": "[str] Continent. One of africa, antarctica, asia, europe, north_america (North America includes the Caribbean and reaches down and includes Panama), oceania, or south_america",
        "occurrence.fields": "[str] Default (all) returns all fields. minimal returns just taxon name, key, latitude, and longitude. Or specify each field you want returned by name, e.g. fields = ['name','latitude','elevation'].",
        "occurrence.mediatype": "[str] Media type. Default is NULL, so no filtering on mediatype. Options: NULL, MovingImage, Sound, and StillImage",
        "occurrence.limit": "[int] Number of results to return. Default: 300",
        "occurrence.offset": "[int] Record to start at. Default: 0",
        "occurrence.facet": "[str] a character vector of length 1 or greater",
        "occurrence.establishmentMeans": "[str] EstablishmentMeans, possible values include: INTRODUCED, INVASIVE, MANAGED, NATIVE, NATURALISED, UNCERTAIN",
        "occurrence.facetMincount": "[int] minimum number of records to be included in the faceting results",
        "occurrence.facetMultiselect": "[bool] Set to True to still return counts for values that are not currently filtered. See examples. Default: False",
        "species.scientificName": "[str] Full scientific name potentially with authorship. (Required)",
        "species.taxonRank": "[str], optional Filter by taxonomic rank. See API reference for available values.",
        "species.usageKey": "[str], optional (NOTE: used for INPUT ONLY, use species.usage.key for OUTPUT) The usage key to look up. When provided, all other fields are ignored.",
        "species.kingdom": "[str], optional Kingdom to match.",
        "species.phylum": "[str], optional Phylum to match.",
        "species.class": "[str], optional Class to match.",
        "species.order": "[str], optional Order to match.",
        "species.superfamily": "[str], optional Superfamily to match.",
        "species.family": "[str], optional Family to match.",
        "species.subfamily": "[str], optional Subfamily to match.",
        "species.tribe": "[str], optional Tribe to match.",
        "species.subtribe": "[str], optional Subtribe to match.",
        "species.genus": "[str], optional Genus to match.",
        "species.subgenus": "[str], optional Subgenus to match.",
        "species.species": "[str], optional Species to match.",
        "species.taxonID": "[str], optional The taxon ID to look up. Matches to a taxonID will take precedence over scientificName values supplied. A comparison of the matched scientific and taxonID is performed to check for inconsistencies.",
        "species.taxonConceptID": "[str], optional The taxonConceptID to match. Matches to a taxonConceptID will take precedence over scientificName values supplied. A comparison of the matched scientific and taxonConceptID is performed to check for inconsistencies.",
        "species.scientificNameID": "[str], optional Matches to a scientificNameID will take precedence over scientificName values supplied. A comparison of the matched scientific and scientificNameID is performed to check for inconsistencies.",
        "species.scientificNameAuthorship": "[str], optional The scientific name authorship to match against.",
        "species.genericName": "[str], optional Generic part of the name to match when given as atomised parts instead of the full name.",
        "species.specificEpithet": "[str], optional Specific epithet to match.",
        "species.infraspecificEpithet": "[str], optional Infraspecific epithet to match.",
        "species.verbatimTaxonRank": "[str], optional Filters by free text taxon rank.",
        "species.exclude": "[str], optional An array of usage keys to exclude from the match.",
        "species.strict": "[bool], optional If set to True, fuzzy matches only the given name, but never a taxon in the upper classification.",
        "species.verbose": "[bool], optional If set to True, shows alternative matches which were considered but then rejected.",
        "species.checklistKey": "[str], optional The key of a checklist to use. Default is the GBIF Backbone taxonomy.",
        "species.usage.key": "[int] A GBIF species identifier (output from species functions)",
    }
    query_template = """```python
from pygbif import species
species.name_backbone(scientificName="Helianthus annuus", kingdom="Plantae")
species.name_backbone(scientificName="Poa", taxonRank="GENUS", family="Poaceae")

# Verbose - gives back alternatives
species.name_backbone(scientificName="Helianthus annuus", kingdom="Plantae", verbose=True)

# Strictness
# If strict=True, then higher taxon ranks will not be returned  when there is a "fuzzy match".
# Higher rank matches will still be returned if the match is exact.
species.name_backbone(scientificName="Poa", kingdom="Plantae", verbose=True, strict=False)
species.name_backbone(scientificName="Helianthus annuus", kingdom="Plantae", verbose=True, strict=True)

# Multiple equal matches
species.name_backbone(scientificName="Oenante")
species.name_backbone(scientificName="Oenante", verbose=True)
species.name_backbone(scientificName="Calopteryx")
species.name_backbone(scientificName="Calopteryx", verbose=True)

# Including authorship in scientificName fixes "Multiple equal matches" note
species.name_backbone(scientificName="Calopteryx splendens (Harris, 1780)")
species.name_backbone(scientificName="Oenanthe L.")

# Match using an alternative checklist 
species.name_backbone(scientificName="Calopteryx splendens", checklistKey="7ddf754f-d193-4cc9-b351-99906754a03b")

import pygbif
from pygbif import occurrences

# turn on caching to speed up repeated queries
pygbif.caching(True, expire_after=3600)

occurrences.search(taxonKey = 3329049)

occurrences.search(taxonKey=3329049, limit=2)

# Never search for species by name directly - always get the taxonKey first
from pygbif import species
key = species.name_backbone(name = 'Ursus americanus', rank='species')['usage']['key']
occurrences.search(taxonKey = key)

# Search by dataset key
occurrences.search(datasetKey='7b5d6a48-f762-11e1-a439-00145eb45e9a', limit=20)

# Search by catalog number
occurrences.search(catalogNumber="49366", limit=20)
# occurrences.search(catalogNumber=["49366","Bird.27847588"], limit=20)

# Use paging parameters (limit and offset) to page. Note the different results
# for the two queries below.
occurrences.search(datasetKey='7b5d6a48-f762-11e1-a439-00145eb45e9a', offset=10, limit=5)
occurrences.search(datasetKey='7b5d6a48-f762-11e1-a439-00145eb45e9a', offset=20, limit=5)

# Many dataset keys
# occurrences.search(datasetKey=["50c9509d-22c7-4a22-a47d-8c48425ef4a7", "7b5d6a48-f762-11e1-a439-00145eb45e9a"], limit=20)

# Search by collector name
res = occurrences.search(recordedBy="smith", limit=20)
[ x['recordedBy'] for x in res['results'] ]

# Many collector names
# occurrences.search(recordedBy=["smith","BJ Stacey"], limit=20)

# recordedByID
occurrences.search(recordedByID="https://orcid.org/0000-0003-1691-239X", limit = 3)

# identifiedByID
occurrences.search(identifiedByID="https://orcid.org/0000-0003-1691-239X", limit = 3)

# Search for many species
splist = ['Cyanocitta stelleri', 'Junco hyemalis', 'Aix sponsa']
keys = [ species.name_suggest(x)[0]['key'] for x in splist ]
out = [ occurrences.search(taxonKey = x, limit=1) for x in keys ]
[ x['results'][0]['speciesKey'] for x in out ]

# Search - q parameter
occurrences.search(q = "kingfisher", limit=20)
## spell check - only works with the `search` parameter
### spelled correctly - same result as above call
occurrences.search(q = "kingfisher", limit=20, spellCheck = True)
### spelled incorrectly - stops with suggested spelling
occurrences.search(q = "kajsdkla", limit=20, spellCheck = True)
### spelled incorrectly - stops with many suggested spellings
###   and number of results for each
occurrences.search(q = "helir", limit=20, spellCheck = True)

# Search on latitidue and longitude
occurrences.search(decimalLatitude=50, decimalLongitude=10, limit=2)

# Search on a bounding box
## in well known text format
occurrences.search(geometry='POLYGON((30.1 10.1, 10 20, 20 40, 40 40, 30.1 10.1))', limit=20)
from pygbif import species
key = species.name_suggest(q='Aesculus hippocastanum')[0]['key']
occurrences.search(taxonKey=key, geometry='POLYGON((30.1 10.1, 10 20, 20 40, 40 40, 30.1 10.1))', limit=20)
## multipolygon
wkt = 'MULTIPOLYGON(((-123 38, -123 43, -116 43, -116 38, -123 38)),((-97 41, -97 45, -93 45, -93 41, -97 41)))'
occurrences.search(geometry = wkt, limit = 20)

# Search on country
occurrences.search(country='US', limit=20)
occurrences.search(country='FR', limit=20)
occurrences.search(country='DE', limit=20)

# Get only occurrences with lat/long data
occurrences.search(taxonKey=key, hasCoordinate=True, limit=20)

# Get only occurrences that were recorded as living specimens
occurrences.search(taxonKey=key, basisOfRecord="LIVING_SPECIMEN", hasCoordinate=True, limit=20)

# Get occurrences for a particular eventDate
occurrences.search(taxonKey=key, eventDate="2013", limit=20)
occurrences.search(taxonKey=key, year="2013", limit=20)
occurrences.search(taxonKey=key, month="6", limit=20)

# Get occurrences based on depth
key = species.name_backbone(name='Salmo salar', kingdom='animals')['usage']['key']
occurrences.search(taxonKey=key, depth="5", limit=20)

# Get occurrences based on elevation
key = species.name_backbone(name='Puma concolor', kingdom='animals')['usage']['key']
occurrences.search(taxonKey=key, elevation=50, hasCoordinate=True, limit=20)

# Get occurrences based on institutionCode
occurrences.search(institutionCode="TLMF", limit=20)

# Get occurrences based on collectionCode
occurrences.search(collectionCode="Floristic Databases MV - Higher Plants", limit=20)

# Get only those occurrences with spatial issues
occurrences.search(taxonKey=key, hasGeospatialIssue=True, limit=20)

# Search using a query string
occurrences.search(q="kingfisher", limit=20)

# Range queries
## See Detail for parameters that support range queries
### this is a range depth, with lower/upper limits in character string
occurrences.search(depth='50,100')

## Range search with year
occurrences.search(year='1999,2000', limit=20)

## Range search with latitude
occurrences.search(decimalLatitude='29.59,29.6')

# Search by specimen type status
## Look for possible values of the typeStatus parameter looking at the typestatus dataset
occurrences.search(typeStatus = 'allotype')

# Search by specimen record number
## This is the record number of the person/group that submitted the data, not GBIF's numbers
## You can see that many different groups have record number 1, so not super helpful
occurrences.search(recordNumber = 1)

# Search by last time interpreted: Date the record was last modified in GBIF
## The lastInterpreted parameter accepts ISO 8601 format dates, including
## yyyy, yyyy-MM, yyyy-MM-dd, or MM-dd. Range queries are accepted for lastInterpreted
occurrences.search(lastInterpreted = '2014-04-01')

# Search by continent
## One of africa, antarctica, asia, europe, north_america, oceania, or south_america
occurrences.search(continent = 'south_america')
occurrences.search(continent = 'africa')
occurrences.search(continent = 'oceania')
occurrences.search(continent = 'antarctica')

# Search for occurrences with images
occurrences.search(mediatype = 'StillImage')
occurrences.search(mediatype = 'MovingImage')
x = occurrences.search(mediatype = 'Sound')
[z['media'] for z in x['results']]

# Query based on issues
occurrences.search(taxonKey=1, issue='DEPTH_UNLIKELY')
occurrences.search(taxonKey=1, issue=['DEPTH_UNLIKELY','COORDINATE_ROUNDED'])
# Show all records in the Arizona State Lichen Collection that cant be matched to the GBIF
# backbone properly:
occurrences.search(datasetKey='84c0e1a0-f762-11e1-a439-00145eb45e9a', issue=['TAXON_MATCH_NONE','TAXON_MATCH_HIGHERRANK'])

# If you pass in an invalid polygon you get hopefully informative errors
### the WKT string is fine, but GBIF says bad polygon
wkt = 'POLYGON((-178.59375 64.83258989321493,-165.9375 59.24622380205539,
-147.3046875 59.065977905449806,-130.78125 51.04484764446178,-125.859375 36.70806354647625,
-112.1484375 23.367471303759686,-105.1171875 16.093320185359257,-86.8359375 9.23767076398516,
-82.96875 2.9485268155066175,-82.6171875 -14.812060061226388,-74.8828125 -18.849111862023985,
-77.34375 -47.661687803329166,-84.375 -49.975955187343295,174.7265625 -50.649460483096114,
179.296875 -42.19189902447192,-176.8359375 -35.634976650677295,176.8359375 -31.835565983656227,
163.4765625 -6.528187613695323,152.578125 1.894796132058301,135.703125 4.702353722559447,
127.96875 15.077427674847987,127.96875 23.689804541429606,139.921875 32.06861069132688,
149.4140625 42.65416193033991,159.2578125 48.3160811030533,168.3984375 57.019804336633165,
178.2421875 59.95776046458139,-179.6484375 61.16708631440347,-178.59375 64.83258989321493))'
occurrences.search(geometry = wkt)

# Faceting
## return no occurrence records with limit=0
x = occurrences.search(facet = "country", limit = 0)
x['facets']

## also return occurrence records
x = occurrences.search(facet = "establishmentMeans", limit = 10)
x['facets']
x['results']

## multiple facet variables
x = occurrences.search(facet = ["country", "basisOfRecord"], limit = 10)
x['results']
x['facets']
x['facets']['country']
x['facets']['basisOfRecord']
x['facets']['basisOfRecord']['count']

## set a minimum facet count
x = occurrences.search(facet = "country", facetMincount = 30000000, limit = 0)
x['facets']

## paging per each faceted variable
### do so by passing in variables like "country" + "_facetLimit" = "country_facetLimit"
### or "country" + "_facetOffset" = "country_facetOffset"
x = occurrences.search(
  facet = ["country", "basisOfRecord", "hasCoordinate"],
  country_facetLimit = 3,
  basisOfRecord_facetLimit = 6,
  limit = 0
)
x['facets']

# requests package options
## There's an acceptable set of requests options (['timeout', 'cookies', 'auth',
## 'allow_redirects', 'proxies', 'verify', 'stream', 'cert']) you can pass
## in via **kwargs, e.g., set a timeout
x = occurrences.search(timeout = 1)
``` 
"""

    sample_occurrence = pygbif.occurrences.search(taxonKey=3329049, limit=5)
    sample_species = pygbif.species.name_backbone(scientificName="Helianthus annuus", kingdom="Plantae")

    return {
      "name": "Global Biodiversity Information Facility (GBIF) via pygbif",
      "description": "Occurrence and species data from the Global Biodiversity Information Facility (GBIF) using the pygbif Python client library.",
      "format": "python",
      "source type": "pygbif",
      "fields": fields,
      "query_template": query_template,
      "sample_occurrence_search_results": sample_occurrence,
      "sample_species_name_backbone_results": sample_species,
      "notes": [
          "Results dicts may not have all fields if they are missing in the GBIF data (e.g., geographical coordinates).",
          "Full documentation for pygbif can be found at https://pygbif.readthedocs.io/en/latest/index.html",
          "The database is very large so pay attention to the order of your queries to maximize performance.",
          "IMPORTANT: If you use limits in your queries, make sure to check the results for completeness and re-query until all results are returned.",
      ]
    }