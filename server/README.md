Trumpet Server
===

## Build it <a id="Build"></a>

This project is based on [Maven](http://maven.apache.org). To build it, you need to ensure 
you have java and maven installed, and then: 

```mvn clean install```

And you can directly import the project in most of the IDE, too.

### RPM

Running `mvn package` (included in `install`) will also generate an RPM.

The RPM can be transformed in .deb via Alien or FPM (see https://www.howtoforge.com/converting_rpm_to_deb_with_alien 
or https://github.com/jordansissel/fpm for more generic conversion) if required.


### Compatibility with Hadoop 2.6.0 and 2.7.0+ <a id="build-compatibility"></a>

[HDFS-7446](https://issues.apache.org/jira/browse/HDFS-7446), released as part of 
[Hadoop 2.7.0 release](https://github.com/aw-altiscale/eco-release-metadata/blob/master/HADOOP/2.7.0/RELEASENOTES.2.7.0.md), 
changed the HDFS-Inotify interface. Trumpet is using a 
[compatibility class](server/src/main/java/org/apache/hadoop/hdfs/inotify/InotifyCompat.java) 
which uses reflection to work around compatibility issues.

Trumpet is tested with 
* CDH 5.2
* CDH 5.3
* HDP 2.2
* Hadoop 2.6.0
* Hadoop 2.7.0


## Run it <a id="Run"></a>

### Trumpet (worker process) <a id="run-trumpet"></a>

Trumpet is a long running process to run alongside the JournalNode. A [supervisord](http://www.supervisord.org) 
config file is also provided (TODO) if you'd like to use supervisord daemon to launch Trumpet.

Once the RPM (or equivalent) is installed, manually launch Trumpet like this:

```
$ /opt/trumpet/bin/trumpet.sh --zk.connect <zk_ip:2181>
```

Note: If you're running CDH, it's highly likely that `dfs.journalnode.name.dir` is not in `/etc/hadoop/conf/hdfs-site.xml`. 
You need to pass `--dfs.edits.dir` parameters to specify manually the directory:

```
$ /opt/trumpet/bin/trumpet.sh --zk.connect <zk_ip:2181> --dfs.edits.dir <root_dir_where_edits_files_are_stored>
```

Note #2: `<root_dir_where_edits_files_are_stored>` should point to the same location than dfs.journalnode.name.dir, 
i.e. excluding `current/BP...TODO...`.

Or copy `/opt/trumpet/conf/trumpet.supervisord.conf` in `/etc/supervisord.d` to have have trumpet 
run as a supervisord process (recommended).

```
sudo cp /opt/trumpet/config/trumpet.supervisor.conf /etc/supervisor/conf.d/trumpet.conf
sudo vi /etc/supervisor/conf.d/trumpet.conf # update <zk_ip:2181> and potentially add --dfs.edits.dir if not found in /etc/hadood/conf/hdfs-site.xml
sudo supervisorctl reread
sudo supervisorctl add trumpet # will start trumpet as autostart is turned on
```


### Tools <a id="run-tools"></a>

#### Validate correctness and completeness

A tool is provided which goes back in time as much it can both in the kafka topic and `dfs.*.name.dir` 
and validate that all the transactions in the edit log dir are in Kafka and match:

```
$ /opt/trumpet/bin/events-validator.sh --zk.connect <zk_ip:2181> --dfs.edits.dir <root_dir_where_edits_files_are_stored> --numofevents 1000000
```

#### Peek Kafka topic

At any time, you can peek the Kafka topic, i.e. retrieve and display in a console the _n_ latest messages:

```
$ /opt/trumpet/bin/peek-events.sh --zk.connect <zk_ip:2181> --numofevents 100
```

#### Workers status

An utility is also provided to display the running trumpet workers as well as which one is the leader

```
$ /opt/trumpet/bin/list-trumpet-workers.sh --zk.connect <zk_ip:2181>
```



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
