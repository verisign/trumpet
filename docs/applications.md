
Trumpet comes out as a new building block in the Hadoop ecosystem 
and unleashes brand new capabilities to HDFS. As it relies mostly on Kafka 
for the distribution of the events to the clients, it is decoupled from the NameNode
and does not impact its operations. It also scales as well as your Kafka cluster.

Below are the steps to let you simply and quickly write your own client applications:


## Dependency

Add the dependency to the Trumpet client in your project

```xml
        <dependency>
            <groupId>com.verisign.vscc.trumpet</groupId>
            <artifactId>trumpet-client</artifactId>
            <version>${trumpet.version}</version>
        </dependency>
```

Hint: the versions follow carefully the tags naming convention. Looks at the available tags in the project to get the version.


## Use it

Now it's as easy as using a Java [Iterable](https://docs.oracle.com/javase/7/docs/api/java/lang/Iterable.html).

```java
String kafkaTopic = ...
String zkConnect = ...
for (Map<String, Object> event : new TrumpetEventStreamer(curatorFramework, kafkaTopic)) {
    ... do something with your event!
}
```

### Delivery guarantee

As Trumpet relies on Kafka, the delivery guarantee of Kafka is inherited for the client side.

`TrumpetEventStreamer` uses internally a [simple consumer](https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example)
and does not store any offset anywhere, the checkpointing process is offloaded to the application writer. 
But a [Kafka consumer group](https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example) can be used instead,
and configured as required to fit you application needs.


## Test it

A test environment is available in the `trumpet-client:tests` jar. To import it inside your maven project, 
use the following dependency:

```xml
        <dependency>
            <groupId>com.verisign.vscc.trumpet</groupId>
            <artifactId>trumpet-client</artifactId>
            <version>${trumpet.version}</version>
            <classifier>tests</classifier>
            <scope>test</scope>
        </dependency>
```

You can then write test classes extending `com.verisign.vscc.hdfs.trumpet.server.IntegrationTest` and you'll have a 
complete running environment, including Zookeeper, Kakfa and 
[mini-hdfs](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/CLIMiniCluster.html).

## Events Format

The events published into Kakfa are JSON dictionary. The rationale behind that is discussed [here](architecture/#events-as-json-dictionary).

Events taken into account are basically all events modifying the NameNode 
[INodeDirectory](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/INodeDirectory.java) 
in-memory structure. The exhaustive list is given below, and they are regrouped in 6 event types: 
`CREATE`, `CLOSE`, `APPEND`, `RENAME`, `METADATA`, `UNLINK`:

* Add -- either `CREATE` or `APPEND`
* Close -- `CLOSE`
* Set Replication -- `METADATA`
* ConcatDelete -- create a sequence of `APPEND`, `UNLINK` and `CLOSE`, but all with the same `txId` (see [notes](architecture/#resume-from-previous-run))
* RenameOld -- `RENAME`
* Rename -- `RENAME`
* Delete -- `UNLINK`
* Mkdir -- `CREATE`
* SetPermissions -- `METADATA`
* SetOwner -- `METADATA`
* Times -- `METADATA`
* Symlink -- `CREATE`
* RemoveXAttr -- `METADATA`
* SetXAttr -- `METADATA`
* SetAcl -- `METADATA`

Find more details about the operation decoding and translation in the class 
[org.apache.hadoop.hdfs.server.namenode.InotifyFSEditLogOpTranslator](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/server/namenode/InotifyFSEditLogOpTranslator.java) 
and 
[org.apache.hadoop.hdfs.inotify.Event](https://github.com/apache/hadoop/blob/trunk/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/inotify/Event.java).

The available attributes in the JSON dict per event types are described below.

* `APPEND`

| Attribute name | Mandatory | Description |
| -------------- |:---------:| ----------- |
| txId           | Yes         | transaction Id |
| eventType      | APPEND    | type of event.  |
| path           |           |             |

* `CLOSE`

| Attribute name | Mandatory | Description |
| -------------- |:---------:| ----------- |
| txId           | Yes         | transaction Id |
| eventType      | CLOSE     | type of event.  |
| path           |           |             |
| fileSize       |           |             |
| timestamp      |           |             |

* `CREATE`

| Attribute name | Mandatory | Description |
| -------------- |:---------:| ----------- |
| txId           | Yes       | transaction Id |
| eventType      | CREATE    | type of event.  |
| iNodeType      |           | Value can be FILE, DIRECTORY or SYMLINK. See org.apache.hadoop.hdfs.inotify.Event.INodeType for more details. |
| path           |           |             |
| ctime          |           |             |
| replication    |           |             |
| ownerName      |           |             |
| groupName      |           |             |
| perms          |           |             |
| symlinkTarget  |           |             |
| overwrite      |           |             |

* `METADATA`

| Attribute name | Mandatory | Description |
| -------------- |:---------:| ----------- |
| txId           | Yes         | transaction Id |
| eventType      | METADATA  | type of event.  |
| path           |           |             |
| metadataType   |           |             |
| mtime          |           |             |
| atime          |           |             |
| replication    |           |             |
| ownerName      |           |             |
| groupName      |           |             |
| perms          |           |             |
| acls           |           |             |
| xAttrs         |           |             |
| xAttrsRemoved  |           |             |

* `RENAME`

| Attribute name | Mandatory | Description |
| -------------- |:---------:| ----------- |
| txId           | Yes         | transaction Id |
| eventType      | RENAME    | type of event.  |
| srcPath        |           |             |
| dstPath        |           |             |
| timestamp      |           |             |

* `UNLINK`

| Attribute name | Mandatory | Description |
| -------------- |:---------:| ----------- |
| txId           | Yes         | transaction Id |
| eventType      | UNLINK    | type of event.  |
| path           |           |             |
| timestamp      |           |             |

## `txId` is a Number!

The JSON object published contains the `txId`, which is actually a proper JSON number.
This means that it might be decoded in Integer or Long, depending on the number.
The proper way to decode it is to make it a `Number` and get the long value:

```
Long txtId = ((Number) event.get(EventAndTxId.FIELD_TXID)).longValue();
```

## Example Application

[com.verisign.vscc.hdfs.trumpet.client.example.TestApp](https://github.com/verisign/trumpet/blob/master/examples/src/main/java/com/verisign/vscc/hdfs/trumpet/client/example/TestApp.java) 
is a sample application, listening to Trumpet and filtering out `_SUCCESS` files, indicating the success of a MapReduce application.

It would be really easy to start a distcp job from here to, for instance, replicate the newly created data into a different remote cluster.

