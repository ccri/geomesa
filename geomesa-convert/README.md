A configurable and extensible library for converting data into SimpleFeatures

## Overview

Converters for various different data formats can be configured and instantiated using the 
```SimpleFeatureConverters``` factory and a target ```SimpleFeatureType```. Currently available converters are: 

 * delimited text
 * fixed width
 * avro
 * json
 * xml
    
The converter allows the specification of fields extracted from the data and transformations on those fields.  Syntax of 
transformations is very much like ```awk``` syntax.  Fields with names that correspond to attribute names in the 
```SimpleFeatureType``` will be directly populated in the result SimpleFeature.  Fields that do not align with 
attributes in the ```SimpleFeatureType``` are assumed to be intermediate fields used for deriving attributes.  Fields 
can reference other fields by name for building up complex attributes.

## Example usage

Suppose you have a ```SimpleFeatureType``` with the following schema: ```phrase:String,dtg:Date,geom:Point:srid=4326``` 
and comma-separated data as shown below.

    first,hello,2015-01-01T00:00:00.000Z,45.0,45.0
    second,world,2015-01-01T00:00:00.000Z,45.0,45.0                                                                                                                                                                    

The first two fields should be concatenated together to form the phrase, the third field should be parsed as a date, and
the last two fields should be formed into a ```Point``` geometry.  The following configuration file defines an 
appropriate converter for taking this csv data and transforming it into our ```SimpleFeatureType```.  

     converter = { 
      type         = "delimited-text",
      format       = "DEFAULT",
      id-field     = "md5($0)",
      fields = [
        { name = "phrase", transform = "concat($1, $2)" },
        { name = "lat",    transform = "$4::double" },
        { name = "lon",    transform = "$5::double" },
        { name = "dtg",    transform = "dateHourMinuteSecondMillis($3)" },
        { name = "geom",   transform = "point($lat, $lon)" }
      ]
     }

The ```id``` of the ```SimpleFeature``` is formed from an md5 hash of the entire record (```$0``` is the original data) 
and the other fields are formed from appropriate transforms.

## Transformation functions

Currently supported transformation functions are listed below.

### String functions
 * ```stripQuotes```
 * ```strlen```
 * ```trim```
 * ```capitalize```
 * ```lowercase```
 * ```regexReplace```
 * ```concat```
 * ```substr```
 * ```toString```
 
### Date functions
 * ```now``` - Current time at ingest
 * ```date``` - custom date parser
 * ```isodate``` - 
 * ```isodatetime```
 * ```basicDateTimeNoMillis```
 * ```dateHourMinuteSecondMillis```
 * ```millisToDate```
 
### Geometry functions

Geometry functions can transform WKT strings, lat/lon pairs, and GeoJSON geometry objects:

Parsing lat/lon

    # config
    { name = "lat", json-type="double", path="$.lat" }
    { name = "lon", json-type="double", path="$.lon" }
    { name = "geom", transform="point($lat, $lon)" }
    
    # data
    {
        "lat": 23.9,
        "lon": 24.2,
    }
     
Parsing lat/lon without creating lat/lon variables
        
    # config
    { name = "geom", transform="point($2::double, $3::double)"
    
    # data
    id,lat,lon,date
    identity1,23.9,24.2,2015-02-03

Parsing WKT

    # config
    { name = "geom", transform="geometry($2)" }
    
    # data 
    ID,wkt,date
    1,POINT(2 3),2015-01-02
       
Parsing Json geometry

    # config
    { name = "geom", json-type = "geometry", path = "$.geometry" }
    
    # data
    {
        id: 1,
        number: 123,
        color: "red",
        "geometry": {"type": "Point", "coordinates": [55, 56]}
     },
    
Available transforms are:

 * ```point```
 * ```linestring```
 * ```polygon```
 * ```geometry```
 
### ID Functions
 * ```string2bytes```
 * ```md5```
 * ```uuid```
 * ```base64```
 
## Data Casting
Data can be cast into various numerical types using the following operators:
 
 * ```::int```
 * ```::long```
 * ```::float```
 * ```::double```
 * ```::boolean```
 
## Parsing Lists and Maps
If your SimpleFeatureType config contains a list or map you can easily configure a transform function to parse it using
the ```parseList``` function which takes either 2 or 3 args

 1. The primitive type of the list (int, string, double, float, boolean, etc)
 2. The reference to parse
 3. Optionally, the list delimiter (defaults to a comma)

Here's some sample CSV data:

    ID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23

For example, an SFT may specific a field:

    {name = "friends", type = "List[String]", index = true}

And a transform to parse the quoted CSV field:

    {name = "friends", transform = "parseList('string', $5)"}
    
Parsing Maps is similar. Take for example this CSV data with a quoted map field:

    1,"1 -> a,2->b,3->c,4->d",2013-07-17,-90.368732,35.3155
    2,"5->e,6->f,7->g,8->h",2013-07-17,-70.970585,42.36211
    3,"9->i,10->j",2013-07-17,-97.599004,30.50901
    
Our field type is:

    numbers:Map[Integer,String]
    
Then we specify a transform:

    { name = "numbers", transform = "parseMap('int -> string', $2)"}

Optionally we can also provide custom list/record and key-value delimiters for a map:
 
      { name = "numbers", transform = "parseMap('int -> string', $2, ',', '->')"}
    

### Avro Path functions
 * ```avroPath```

## Extending the converter library

There are two ways to extend the converter library - adding new transformation functions and adding new data formats.

### Adding new transformation functions

To add new transformation functions, create a ```TransformationFunctionFactory``` and register it in 
```META-INF/services/org.locationtech.geomesa.convert.TransformationFunctionFactory```.  For example, here's how to add 
a new transformation function that computes a SHA-256 hash.

```scala
class SHAFunctionFactory extends TransformerFunctionFactory {
  override def functions = Seq(sha256)
  val sha256fn = TransformerFn("sha256") { args => Hashing.sha256().hashBytes(args(0).asInstanceOf[Array[Byte]]) }
}
```

The ```sha256``` function can then be used in a field as shown.

```
   ...
   fields: [
      { name = "hash", transform = "sha256($0)" }
   ]
```

### Adding new data formats

To add new data formats, implement the ```SimpleFeatureConverterFactory``` and ```SimpleFeatureConverter``` interfaces 
and register them in ```META-INF/services``` appropriately.  See 
```org.locationtech.geomesa.convert.avro.Avro2SimpleFeatureConverter``` for an example.

## Full Example Using GeoMesa Tools

The following example can be used with geomesa tools:

    geomesa ingest -u <user> -p <pass> -i <instance> -z <zookeepers> -conf example.conf example.csv 

Sample csv file: ```example.csv```

    ID,Name,Age,LastSeen,Friends,Lat,Lon
    23623,Harry,20,2015-05-06,"Will, Mark, Suzan",-100.236523,23
    26236,Hermione,25,2015-06-07,"Edward, Bill, Harry",40.232,-53.2356
    3233,Severus,30,2015-10-23,"Tom, Riddle, Voldemort",3,-62.23
    
A config file: ```example.conf```

    sft = {
      type-name = "renegades"
      attributes = [
        {name = "id", type = "Integer", index = false},
        {name = "name", type = "String", index = true},
        {name = "age", type = "Integer", index = false},
        {name = "lastseen", type = "Date", index = true},
        {name = "friends", type = "List[String]", index = true},
        {name = "geom", type = "Point", index = true, srid = 4326, default = true}
      ]
    },
    converter = {
      type = "delimited-text",
      format = "CSV",
      options {
        skip-lines = 1
      },
      id-field = "toString($id)",
      fields = [
        {name = "id", transform = "$1::int"},
        {name = "name", transform = "$2::string"},
        {name = "age", transform = "$3::int"},
        {name = "lastseen", transform = "$4::date"},
        {name = "friends", transform = "parseList('string', $5)"},
        {name = "lon", transform = "$6::double"},
        {name = "lat", transform = "$7::double"},
        {name = "geom", transform = "point($lon, $lat)"}
      ]
    }