
Trumpet Changelog.
## Version 2.6

* Use jdk 8
* Add Kafka 1.0 support
* Implement the new Consumer for Kafka 0.10
* Add support to security with SASL_SSL
* Deprecated the TrumpetEventStreamer and create a new TrumpetEventConsumer
* Possibility to specify the consumer.properties and producer.properties files

## Version 2.3

* [#PR1]() Move examples in a dedicated subproject
* Add Trie data structure for efficient path filtering
* Remove dependency on github.com/jstanier/kafka-broker-discovery
* Add HDP 2.3.4 support
* Clean up dependencies -- importing trumpet-client in downstream project is safe
* Split TrumpetEventStreamer in infinite (relying on Kafka Group Consumer) and Bounded (based on Kafka Simple Consumer)

## Version 2.2

* Initial Public Release
