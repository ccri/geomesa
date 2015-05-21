#GeoMesa KafkaDataStore
The `KafkaDataStore` is an implementation of the geotools `DataStore` interface that is backed by a Kafka message broker.
The implementation supports the ability for Feature producers to persist data into the data store and for consumers to 
pull data from the datastore.  The data store supports both live and replay consumers modes.  Consumers operating in 
live mode pull data from the end of the message (latest time) queue while consumers operating in replay mode pull data 
from a specified time interval in the past.  

##Usage/Configuration
Clients of the `KafkaDataStore` select between live mode and replay mode through hints provided by the SimpleFeatureType
object passed to `createSchema(SimpleFeatureType)`.  These hints are added through the use of the following helper
methods:

```
KafkaDataStoreHelper.prepareForLive(String, SimpleFeatureType)
KafkaDataStoreHelper.prepareForReplay(ReplayConfig, SimpleFeatureType)
```