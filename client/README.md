Trumpet Client
===

## Build it <a id="Build"></a>

This project is based on [Maven](http://maven.apache.org). To build it, you need to ensure 
you have java and maven installed, and then: 

```mvn clean install```

And you can directly import the project in most of the IDE, too.


## Use it <a id="Use"></a>

Trumpet comes out as a new building block in the Hadoop ecosystem 
and unleashes brand new capabilities to HDFS. As it relies mostly on Kafka 
for the distribution of the events to the clients, it scales pretty well.

One really promising use-case built on top of Trumpet in near-real time data replication 
from a source HDFS cluster to a destination HDFS cluster.

TODO: other use-cases here


# Change log <a id="ChangeLog"></a>

See [CHANGELOG](../CHANGELOG.md).


# Contributing to trumpet <a id="Contributing"></a>

Code contributions, bug reports, feature requests etc. are all welcome.

If you are new to GitHub please read [Contributing to a project](https://help.github.com/articles/fork-a-repo) 
for how to send patches and pull requests to trumpet.


# Authors <a id="Authors"></a>

* [Benoit Perroud](https://github.com/killerwhile)
* [Hari Kuppuswamy](https://github.com/hariprasad-k)
* [Paula Morais](https://github.com/psilvaro)


# License <a id="License"></a>

Copyright © 2014-2015 [VeriSign, Inc.](http://www.verisigninc.com/)

See [LICENSE](../LICENSE) for licensing information.
