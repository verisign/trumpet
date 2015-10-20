
Trumpet is an highly-available, fault-tolerant, non intrusive and scalable 
[INotify](http://en.wikipedia.org/wiki/Inotify)-like building block for Hadoop [HDFS](http://hadoop.apache.org/).

---

# Context

In real world Hadoop deployments it's common to run several HDFS clusters. 
When doing so, one of the challenge is to replicate the data from one side to another while finding 
the right trade-offs between the time before the data is replicated 
and impact on the existing infrastructure.

This project originated from work at [VeriSign](https://www.verisigninc.com) and presented at [Hadoop Summit Amsterdam in 2014](http://2014.hadoopsummit.org/) 
(Hadoop Event Notification System, see [http://hortonworks.com/blog/congrats-hadoop-summit-community-choice-winners/](http://hortonworks.com/blog/congrats-hadoop-summit-community-choice-winners/)). 
It in turn brought inspiration to the community to build the HDFS-INotify building block, 
known as [HDFS-6634](https://issues.apache.org/jira/browse/HDFS-6634) and [HDFS-7446](https://issues.apache.org/jira/browse/HDFS-7446).

But HDFS-INotify is *intrusive* to the NameNode by design, i.e. it's built as an RPC command hitting directly the NameNode. 
This has a bunch of advantages like allowing to have no other dependencies as well as enable (in the future) further processing 
directly on the in-memory directory structure. But in real life busy production clusters, where the first 
goal of an Hadoop [SRE](http://en.wikipedia.org/wiki/Reliability_engineering)s is to protect the [NameNode](http://wiki.apache.org/hadoop/NameNode), 
running a more decoupled version of HDFS-INotify would probably be preferred (see [Notes](#notes) below)

So was born the project called Trumpet, in reference of the noise the elephant is producing when shouting.


# Features

Trumpet put a strong focus on the following areas:

* **Scalable** -- relies on [Kafka](http://kafka.apache.org) for events distribution,
* **Non intrusive** with low footprint -- runs alongside (and not in) the NameNode or the JournalNode,
* **Highly available** and **fault tolerant** -- several instances run a leader election in [Zookeeper](http://zookeeper.apache.org) and recover from previous state,
* **Rolling upgrade** ready -- knows how to deal with HDFS rolling upgrade,
* **Simple** -- writes your own client application in seconds,
* **Ops friendly** -- provides tools to ensure completeness and correctness of the broadcast,
* **Compatibility** -- runs (i.e. tested) on Hadoop 2.6, 2.7, HDP 2.2, 2.3 and CDH 5.2, 5.3, 5.4,
* **Near realtime** -- detects new transactions within milliseconds\*.

To dig into Trumpet's features and how it works, please go to the [architecture](architecture/) page.


# Quickstart

## Server

1. Clone the project and build it
  ```mvn clean install```
2. Install the RPM generated in `server/target`
3. Create a topic (Trumpet does not create the topic itself). For instance, for Kakfa 0.8.2-cp
  ```kafka-topics --create --zookeeper <zk_ip:2181> --replication-factor 4 --partitions 1 --topic hdfs.inotify.events```
4. Start it.
  ```/opt/trumpet/server/bin/trumpet.sh --zk.connect <zk_ip:2181>```

Or user the [supervisord](http://www.supervisord.org) [config file](https://github.com/verisign/trumpet/tree/master/server/src/main/config/trumpet-server.ini) provided.

For more details about the server side, please go to the [installation](installation/) page.

## Client Application

Add the dependency to the Trumpet client in your project
```xml
        <dependency>
            <groupId>com.verisign.vscc.trumpet</groupId>
            <artifactId>trumpet-client</artifactId>
            <version>${trumpet.version}</version>
        </dependency>
```

Hint: the versions follow carefully the tags naming convention. Looks at the available tags in the project to get the version.


Now it's as easy as using a Java [Iterable](https://docs.oracle.com/javase/7/docs/api/java/lang/Iterable.html).

```java
String kafkaTopic = ...
String zkConnect = ...
for (Map<String, Object> event : new TrumpetEventStreamer(curatorFramework, kafkaTopic)) {
    ... do something with your event!
}
```
One can also specify a start transaction id to start reading from a known point in time, as well as a stop transaction id
to read only a subset of transactions.

For more details about the client side, please go to the [applications](applications/) page.


# Contributing to Trumpet

Code contributions, bug reports, feature requests etc. are all welcome.

If you are new to [GitHub](https://github.com) please read [Contributing to a project](https://help.github.com/articles/fork-a-repo) 
for how to send patches and pull requests to Trumpet.


# Authors

The initial authors of Trumpet are: 

* [Benoit Perroud](https://github.com/killerwhile)
* [Hari Kuppuswamy](https://github.com/hariprasad-k)
* [Paula Morais](https://github.com/psilvaro)

And special thanks to [James Thomas](https://issues.apache.org/jira/secure/ViewProfile.jspa?name=james.thomas) 
and [Colin Patrick McCabe](https://issues.apache.org/jira/secure/ViewProfile.jspa?name=cmccabe) 
who wrote the community version of HDFS INotify.


<a id="notes">*Notes*</a>

Cloudera [reverted](https://github.com/cloudera/hadoop-common/commit/15b703c8725733b7b2813d2325659eb7d57e7a3f) [HDFS-7929](https://issues.apache.org/jira/browse/HDFS-7929) in [CDH 5.4.2](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_rn_fixed_in_542.html) because of issues with INotify in rolling upgrades.

