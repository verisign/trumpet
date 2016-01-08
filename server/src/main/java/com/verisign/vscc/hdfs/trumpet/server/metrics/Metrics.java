package com.verisign.vscc.hdfs.trumpet.server.metrics;

import com.codahale.metrics.*;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetLeader;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetServer;

import java.util.concurrent.atomic.AtomicLong;

import static com.codahale.metrics.MetricRegistry.name;

public class Metrics {

    private static final MetricRegistry REGISTRY = new MetricRegistry();

    public static MetricRegistry getRegistry() {
        return REGISTRY;
    }

    private static final String leadershipUptime = name(TrumpetLeader.class, "leadership", "uptime");
    private static final Counter leadershipCounter = getRegistry().counter(name(TrumpetLeader.class, "leadership", "count"));
    private static final String uptime = name(TrumpetServer.class, "uptime");
    private static final String lastTransactionIdName = name(TrumpetServer.class, "transactions", "lastId");
    private static final Meter transactionInotifyMeter = getRegistry().meter(name(TrumpetServer.class, "transactions", "inotify"));
    private static final Meter transactionAllMeter = getRegistry().meter(name(TrumpetServer.class, "transactions", "all"));
    private static final Meter transactionKafkaMeter = getRegistry().meter(name(TrumpetServer.class, "transactions", "kafka"));
    private static final Timer sleepTimer = getRegistry().timer(name(TrumpetLeader.class, "sleep"));
    private static final Timer processingTimer = getRegistry().timer(name(TrumpetLeader.class, "processing"));
    private static final Timer editLogFileTimer = getRegistry().timer(name(TrumpetLeader.class, "editlogfile"));
    private static final Meter noTxFileFoundMeter = getRegistry().meter(name(TrumpetLeader.class, "noTxFileFound"));

    public static void leadershipUptime() {
        getRegistry().remove(leadershipUptime);
        final long starttime = System.currentTimeMillis();
        getRegistry().register(leadershipUptime, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return System.currentTimeMillis() - starttime;
            }
        });
    }

    public static Counter leadershipCounter() {
        return leadershipCounter;
    }

    public static void uptime() {
        final long starttime = System.currentTimeMillis();
        getRegistry().register(uptime, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return System.currentTimeMillis() - starttime;
            }
        });
    }

    public static void lastTransactionId(final AtomicLong lastTxId) {
        // remove a previously added instance,
        getRegistry().remove(lastTransactionIdName);

        getRegistry().register(lastTransactionIdName, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return lastTxId.longValue();
            }
        });
    }

    public static Meter inotifyTransaction() {
        return transactionInotifyMeter;
    }

    public static Meter allTransaction() {
        return transactionAllMeter;
    }

    public static Meter kafkaTransaction() {
        return transactionKafkaMeter;
    }

    public static Timer sleep() {
        return sleepTimer;
    }

    public static Timer process() {
        return processingTimer;
    }

    public static Timer editLogFile() {
        return editLogFileTimer;
    }

    public static Meter noTxFileFound() {
        return noTxFileFoundMeter;
    }

    public static void close() {
        getRegistry().remove(uptime);
    }

}
