#!/bin/bash
############################################################################
#
# The information in this document is proprietary
# to VeriSign and the VeriSign Product Development.
# It may not be used, reproduced or disclosed without
# the written approval of the General Manager of
# VeriSign Product Development.
#
# PRIVILEGED AND CONFIDENTIAL
# VERISIGN PROPRIETARY INFORMATION
# REGISTRY SENSITIVE INFORMATION
#
# Copyright (c) 2015 VeriSign, Inc.  All rights reserved.
#
############################################################################
#
#  Simple helper script to launch any application in Hadoop context
# 
############################################################################

# Make sure umask is sane
umask 022

# Automatically mark variables and functions which are modified or created
# for export to the environment of subsequent commands.
set -o allexport

if [ "$(whoami)" != "hdfs" ]
then
    echo "Must be run as hdfs user!"
    exit 125
fi

# Set up a default search path.
PATH="/sbin:/usr/sbin:/bin:/usr/bin"
export PATH

scriptdir=`dirname $0`/..
HOMEDIR=${HOMEDIRPRIME:-`readlink -f $scriptdir`}

appdir=$HOMEDIR/..
APPDIR=${APPDIRPRIME:-`readlink -f $appdir`}

SRCDIR=${SRCDIRPRIME:-$HOMEDIR/src}
LIBDIR=${LIBDIRPRIME:-$HOMEDIR/lib}
CONFDIR=${SRCONFDIRPRIME:-$HOMEDIR/config}

# Update path so scripts under bin are available
PATH=$HOMEDIR/bin:$PATH

cd $HOMEDIR

## Generate classpath from libraries in $LIBDIR, and add $CONFDIR if required
OUR_CLASSPATH=$(find $LIBDIR -type f -name "*.jar" | paste -sd:)
if [ -d "$CONFDIR" ]; then
    OUR_CLASSPATH=$CONFDIR:$OUR_CLASSPATH
fi

# Remote Debug Java Opts
#JAVA_OPTS="-Xdebug -Xnoagent -Xrunjdwp:transport=dt_socket,address=7778,server=y,suspend=y"

# Memory-related Opts
#JAVA_OPTS="-XX:+HeapDumpOnOutOfMemoryError -verbose:gc"

# Start up size for memory allocation pool for java VM.
MS=128m

# Max size of memory allocation pool for java VM.
MX=2048m

JAVA_OPTS="$JAVA_OPTS -Xms$MS -Xmx$MX -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:-CMSConcurrentMTEnabled -XX:CMSInitiatingOccupancyFraction=70 -XX:+CMSParallelRemarkEnabled -XX:+DoEscapeAnalysis"
JAVA_OPTS="$JAVA_OPTS -Dlog4j.configuration=log4j-production.properties"

if [ -f $HOMEDIR/config/kafka_client_jaas.conf ]
then
    JAVA_OPTS="${JAVA_OPTS} -Djava.security.auth.login.config=$HOMEDIR/config/kafka_client_jaas.conf"
fi

export HADOOP_USER_CLASSPATH_FIRST=true

export HADOOP_CLASSPATH=$OUR_CLASSPATH
export YARN_OPTS="-Djava.net.preferIPv4Stack=true $JAVA_OPTS"

command="yarn jar $LIBDIR/trumpet-server.jar com.verisign.vscc.hdfs.trumpet.server.TrumpetServerCLI"

echo $command $@
$command $@
