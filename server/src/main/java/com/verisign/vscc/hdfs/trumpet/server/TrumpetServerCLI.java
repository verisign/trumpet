package com.verisign.vscc.hdfs.trumpet.server;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import com.verisign.vscc.hdfs.trumpet.server.metrics.Metrics;
import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.InfluxdbReporter;
import metrics_influxdb.api.measurements.CategoriesMetricMeasurementTransformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TrumpetServerCLI extends AbstractAppLauncher {

    public static final String OPTION_DFS_EDITS_DIR = "dfs.edits.dir";
    public static final String OPTION_BASE_THROTTLE_TIME_MS = "base.throttle.time.ms";
    public static final String OPTION_TSDB_SERVER_HOST_PORT = "tsdb.server.hostport";
    public static final String OPTION_TSDB_DB = "tsdb.db";
    public static final String OPTION_KAFKA_REQUIRED_ACKS = "kafka.required.acks";
    public static final String OPTION_METRICS_PREFIX_DEFAULT = "trumpet." + getHostname();

    private final CountDownLatch latch = new CountDownLatch(1);
    private boolean initialized = false;
    private GraphiteReporter graphiteReporter;
    private TrumpetServer trumpetServer;
    private JmxReporter jmxReporter;

    private ScheduledReporter metricsReporter;
    private TrumpetServer trumpetServer;
    private JmxReporter jmxReporter;

    private File dfsEditsDir;
    private long baseThrottleTimeMs;
    private int kafkaRequiredAcks;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TrumpetServerCLI(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        String tmpDfsEditsDir = null;
        if (getOptions().has(OPTION_DFS_EDITS_DIR)) {
            tmpDfsEditsDir = (String) getOptions().valueOf(OPTION_DFS_EDITS_DIR);
        } else {
            tmpDfsEditsDir = getConf().get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY);
            if (tmpDfsEditsDir != null) {
                final String[] dirs = tmpDfsEditsDir.split(",");
                final Path p = new Path(dirs[0]);
                tmpDfsEditsDir = p.toUri().getPath();
            }
        }
        if (tmpDfsEditsDir == null) {
            LOG.error("No " + OPTION_DFS_EDITS_DIR + " directory set. Please pass it to the command line with --"
                    + OPTION_DFS_EDITS_DIR + " or set " + DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + " in your hadoop config");
            return ReturnCode.ERROR_WITH_DFS_EDIT_DIR;
        }

        dfsEditsDir = new File(tmpDfsEditsDir);
        if (!dfsEditsDir.isDirectory()) {
            LOG.error("Directory " + dfsEditsDir + " does not seem to exist. Please set the directory in your " +
                    "hadoop config (key is " + DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + ")");
            return ReturnCode.ERROR_WITH_DFS_EDIT_DIR;
        }
        if (!dfsEditsDir.canRead()) {
            LOG.error("Directory " + dfsEditsDir + " can be read. Please correct the dfset the directory in your " +
                    "hadoop config (key is " + DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + ")");
            return ReturnCode.ERROR_WITH_DFS_EDIT_DIR;
        }

        baseThrottleTimeMs = Long.parseLong((String) getOptions().valueOf(OPTION_BASE_THROTTLE_TIME_MS));
        kafkaRequiredAcks = Integer.parseInt((String) getOptions().valueOf(OPTION_KAFKA_REQUIRED_ACKS));

        final JmxReporter localJmxReporter = JmxReporter.forRegistry(Metrics.getRegistry()).build();
        jmxReporter = localJmxReporter;
        localJmxReporter.start();

        final ScheduledReporter localReporter;
        if (getOptions().has(OPTION_TSDB_SERVER_HOST_PORT)) {
            String tsdbServerPort = (String) getOptions().valueOf(OPTION_TSDB_SERVER_HOST_PORT);
            String tsdbDB = (String) getOptions().valueOf(OPTION_TSDB_DB);
            String[] serverParts = tsdbServerPort.split(":");
            int port = Integer.parseInt(serverParts.length > 1 ? serverParts[1] : "8086");
            localReporter = InfluxdbReporter.forRegistry(Metrics.getRegistry())
                    .protocol(new HttpInfluxdbProtocol(serverParts[0], port, tsdbDB))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .skipIdleMetrics(false)
                    .tag("server", InetAddress.getLocalHost().getHostName())
                    .transformer(new CategoriesMetricMeasurementTransformer("module", "artifact"))
                    .build();
            localReporter.start(10, TimeUnit.SECONDS);
        } else {
            localReporter = null;
        }
        metricsReporter = localReporter;
        /*
        final GraphiteReporter localGraphiteReporter;
        if (getOptions().has(OPTION_TSDB_SERVER_HOST_PORT)) {
            String graphiteServer = (String) getOptions().valueOf(OPTION_TSDB_SERVER_HOST_PORT);
            String grahitePrefix = (String) getOptions().valueOf(OPTION_TSDB_DB);
            String[] serverParts = graphiteServer.split(":");
            final Graphite graphite = new Graphite(
                    new InetSocketAddress(serverParts[0],
                            Integer.parseInt(serverParts.length > 1 ? serverParts[1] : "2003")));
            localGraphiteReporter = GraphiteReporter.forRegistry(Metrics.getRegistry())
                    .prefixedWith(grahitePrefix)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);
            localGraphiteReporter.start(1, TimeUnit.MINUTES);
        } else {
            localGraphiteReporter = null;
        }
        graphiteReporter = localGraphiteReporter;
*/
        Metrics.uptime();

        final TrumpetServer localTrumpetServer = new TrumpetServer(getCuratorFrameworkUser(), getConf(), getTopic(), dfsEditsDir, kafkaRequiredAcks, baseThrottleTimeMs);
        trumpetServer = localTrumpetServer;
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {

                LOG.info("Trumpet shutdown from external signal received.");

                try {
                    localTrumpetServer.close();
                } catch (IOException e) {
                    // well, can't really recover here, shutting down anyway...
                    e.printStackTrace();
                } finally {
                    localJmxReporter.stop();
                    if (localReporter != null) {
                        localReporter.stop();
                    }
                    Metrics.close();
                }
            }
        });


        initialized = true;

        LOG.info("Application is launching. Moving verbosity to log file.");

        localTrumpetServer.run();

        LOG.info("Application terminated. Shutdown complete.");

        return ReturnCode.ALL_GOOD;

    }

    @Override
    protected void initParser() {
        getParser().accepts(OPTION_DFS_EDITS_DIR, "Root directory where the edits files are stored " +
                "(i.e. without <serviceId>/current). Mandatory if " +
                DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + " is not set in /etc/hadoop/conf/hdfs-site.xml.")
                .withRequiredArg();
        getParser().accepts(OPTION_BASE_THROTTLE_TIME_MS, "Min throttle time increment when no transactions are found. " +
                "The bigger value, the less resources will be used but the more time before events might be published.")
                .withRequiredArg().defaultsTo(String.valueOf(TrumpetServer.DEFAULT_BASE_THROTTLE_TIME_MS));
        getParser().accepts(OPTION_TSDB_SERVER_HOST_PORT, "Graphite server host and port where to report metrics, if set. Port default to 2003 if not set.")
                .withRequiredArg();
        getParser().accepts(OPTION_TSDB_DB, "Prefix to add on the metrics reported to Graphite.")
                .withRequiredArg().defaultsTo(OPTION_METRICS_PREFIX_DEFAULT);
        getParser().accepts(OPTION_KAFKA_REQUIRED_ACKS, "Kafka request.required.acks property. 0=none (async), 1=leader, -1=all. See kafka documentation for more details.")
                .withRequiredArg().defaultsTo(String.valueOf(SimpleConsumerHelper.DEFAULT_REQUIRED_ACKS));
    }


    @Override
    protected void internalClose() throws IOException {
        latch.countDown();
        LOG.info("Trumpet shutdown internalClose");

        try {
            trumpetServer.close();
        } catch (IOException e) {
            // well, can't really recover here, shutting down anyway...
            e.printStackTrace();
        } finally {
            jmxReporter.stop();
            if (metricsReporter != null) {
                metricsReporter.stop();
            }
            Metrics.close();
        }
    }

    public boolean isInitialized() {
        return initialized;
    }

    protected class ReturnCode extends AbstractAppLauncher.ReturnCode {

        public static final int ERROR_WITH_DFS_EDIT_DIR = 5;

    }
}
