
# Project structure

The current project is composed of 4 sub-projects:

1. [`common`](../common)  
  The `common` sub-project is where the reusable code is shared across the projects.

2. [`server`](../server)  
  Trumpet server, i.e. reading the hdfs edits log files. Build this project to [deploy and run Trumpet](#Build_it)

3. [`client`](../client)  
  Example of client application, reading from Kakfa and doing someting meaningful. This sub-project also
  contains the required dependencies to write [client applications](applications/).

4. [`docs`](../docs)  
  This documentation.


# Build it

This project is based on [Maven](http://maven.apache.org). To build it, you need to ensure 
you have java and maven installed, and then: 

```bash
mvn clean install
```

Available options, in addition to the standard maven options:

* `-Phadoop26` (default), `-Phadoop27`, `-Pcdh52`, `-Pcdh53`, `-Pcdh54` or `-Phdp22`. See [Compatibility](#Compatibility) for details.
* `-DrpmRelease=1` to change the RPM build release number. See [RPM](#RPM) for details.

## RPM

Running `mvn package` (included in the `install` goal) will generate an RPM. `find server/target -name "*.rpm"` to list 
the exact path and name of the server RPM.

The RPM can be transformed in .deb via Alien or FPM (see [https://www.howtoforge.com/converting_rpm_to_deb_with_alien](https://www.howtoforge.com/converting_rpm_to_deb_with_alien) 
or [https://github.com/jordansissel/fpm](https://github.com/jordansissel/fpm) for more generic conversion) if required.

The RPM is installing Trumpet under `/opt/trumpet/server/`, with the following directory structure:

```bash
/opt/trumpet/server/
  +- bin/
  |  +- events-validator.sh
  |  +- peek-events.sh
  |  +- status.sh
  |  -- trumpet.sh
  +- conf/
  |  -- logback-production.xml
  +- lib/
  |  +- server-<VERSION>.jar
  |  +- server.jar -> server-<VERSION>.jar
  |  -- ... all the other jars ...
  -- logs/
     +- trumpet.log
     +- stdout
     -- stderr
```

While building Trumpet, you can add `-DrpmRelease=123` as a maven command parameter to change the RPM build number (by default _1_)

Note: if you just want to build a specific version of the server RPM, without rebuilding everything, you need to run the following:

```bash
mvn build-helper:parse-version rpm:attached-rpm -Phadoop27 -DrpmRelease=123 -f server
```

## Compatibility

[HDFS-7446](https://issues.apache.org/jira/browse/HDFS-7446), released as part of 
[Hadoop 2.7.0 release](https://github.com/aw-altiscale/eco-release-metadata/blob/master/HADOOP/2.7.0/RELEASENOTES.2.7.0.md), 
changed the HDFS-Inotify interface. In order to achieve multi-version compatibility, Trumpet is using a 
[compatibility class](https://github.com/verisign/trumpet/tree/master/server/src/main/java/org/apache/hadoop/hdfs/inotify/InotifyCompat.java) 
which uses reflection to work around compatibility issues.

Trumpet is tested with:

* Hadoop 2.6 -- `mvn clean install -Phadoop26` (default)
* Hadoop 2.7 -- `mvn clean install -Phadoop27`
* CDH 5 -- `mvn clean install -Pcdh52`, `mvn clean install -Pcdh53` or `mvn clean install -Pcdh54`
* HDP 2.2, 2.3 -- `mvn clean install -Phdp22`, `mvn clean install -Phdp23`


# Run it

Trumpet server is a long running process to run alongside the JournalNode. A [supervisord](http://www.supervisord.org) 
[config file](https://github.com/verisign/trumpet/tree/master/server/src/main/config/trumpet-server.ini) is also provided if you'd like to use supervisord daemon to launch Trumpet.

Once the RPM (or equivalent) is installed, manually launch Trumpet like this:

```bash
$ /opt/trumpet/server/bin/trumpet.sh --zk.connect <zk_ip:2181>
```

Note: If you're running CDH, it's highly likely that `dfs.journalnode.name.dir` is not in `/etc/hadoop/conf/hdfs-site.xml`. 
You need to manually set `--dfs.edits.dir` parameters to specify manually the directory:

```bash
$ /opt/trumpet/server/bin/trumpet.sh --zk.connect <zk_ip:2181> --dfs.edits.dir <root_dir_where_edits_files_are_stored>
```

Note #2: `<root_dir_where_edits_files_are_stored>` should point to the same location than `dfs.journalnode.name.dir`, 
i.e. excluding `<nameservice>/current`.

Or copy `/opt/trumpet/server/conf/trumpet-server.ini` in `/etc/supervisord.d` to have have Trumpet 
run as a supervisord process (recommended).

```bash
sudo cp /opt/trumpet/server/config/trumpet-server.ini /etc/supervisord.d/
sudo vi /etc/supervisord.d/trumpet-server.ini # update <zk_ip:2181> and potentially add --dfs.edits.dir if not found in /etc/hadood/conf/hdfs-site.xml
sudo supervisorctl reread
sudo supervisorctl add trumpet # will start Trumpet as autostart is turned on
```

Note #3: all the scripts can be run with `--help` to get additional info
