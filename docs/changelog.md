
Trumpet Changelog.

## Version 2.3

* [#PR1]() Move examples in a dedicated subproject
* Add Trie data structure for efficient path filtering
* Remove dependency on github.com/jstanier/kafka-broker-discovery
* Add HDP 2.3.4 support
* Clean up dependencies -- importing trumpet-client in downstream project is safe
* Split TrumpetEventStreamer in infinite (relying on Kafka Group Consumer) and Bounded (based on Kafka Simple Consumer)

## Version 2.2

* Initial Public Release
