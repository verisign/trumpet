package com.verisign.vscc.hdfs.trumpet.server;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.server.metrics.Metrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TrumpetServerCLI extends AbstractAppLauncher {

    public static final String OPTION_DFS_EDITS_DIR = "dfs.edits.dir";
    public static final String OPTION_BASE_THROTTLE_TIME_MS = "base.throttle.time.ms";
    public static final String OPTION_GRAPHITE_SERVER_HOST_PORT = "graphite.server.hostport";
    public static final String OPTION_GRAPHITE_PREFIX = "graphite.prefix";
    public static final String OPTION_GRAPHITE_PREFIX_DEFAULT = "trumpet." + getHostname();

    private final CountDownLatch latch = new CountDownLatch(1);
    private boolean initialized = false;

    private File dfsEditsDir;
    private long baseThrottleTimeMs;

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
            System.err.println("No " + OPTION_DFS_EDITS_DIR + " directory set. Please pass it to the command line with --"
                    + OPTION_DFS_EDITS_DIR + " or set " + DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + " in your hadoop config");
            return ReturnCode.ERROR_WITH_DFS_EDIT_DIR;
        }

        dfsEditsDir = new File(tmpDfsEditsDir);
        if (!dfsEditsDir.isDirectory()) {
            System.err.println("Directory " + dfsEditsDir + " does not seem to exist. Please set the directory in your " +
                    "hadoop config (key is " + DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + ")");
            return ReturnCode.ERROR_WITH_DFS_EDIT_DIR;
        }
        if (!dfsEditsDir.canRead()) {
            System.err.println("Directory " + dfsEditsDir + " can be read. Please correct the dfset the directory in your " +
                    "hadoop config (key is " + DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + ")");
            return ReturnCode.ERROR_WITH_DFS_EDIT_DIR;
        }

        baseThrottleTimeMs = Long.parseLong((String) getOptions().valueOf(OPTION_BASE_THROTTLE_TIME_MS));

        final JmxReporter jmxReporter = JmxReporter.forRegistry(Metrics.getRegistry()).build();
        jmxReporter.start();

        final GraphiteReporter graphiteReporter;
        if (getOptions().has(OPTION_GRAPHITE_SERVER_HOST_PORT)) {
            String graphiteServer = (String) getOptions().valueOf(OPTION_GRAPHITE_SERVER_HOST_PORT);
            String grahitePrefix = (String) getOptions().valueOf(OPTION_GRAPHITE_PREFIX);
            String[] serverParts = graphiteServer.split(":");
            final Graphite graphite = new Graphite(
                    new InetSocketAddress(serverParts[0],
                            Integer.parseInt(serverParts.length > 1 ? serverParts[1] : "2003")));
            graphiteReporter = GraphiteReporter.forRegistry(Metrics.getRegistry())
                    .prefixedWith(grahitePrefix)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(MetricFilter.ALL)
                    .build(graphite);
            graphiteReporter.start(1, TimeUnit.MINUTES);
        } else { graphiteReporter = null; }

        Metrics.uptime();

        final TrumpetServer trumpetServer = new TrumpetServer(getCuratorFrameworkUser(), getConf(), getTopic(), dfsEditsDir, baseThrottleTimeMs);

        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {

                System.out.println("Trumpet shutdown from external signal received.");

                try {
                    trumpetServer.close();
                } catch (IOException e) {
                    // well, can't really recover here, shutting down anyway...
                    e.printStackTrace();
                } finally {
                    jmxReporter.stop();
                    if (graphiteReporter != null) {
                        graphiteReporter.stop();
                    }
                }
            }
        });


        initialized = true;

        System.out.println("Application is launching. Moving verbosity to log file.");

        trumpetServer.run();

        System.out.println("Application terminated. Shutdown complete.");

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
        getParser().accepts(OPTION_GRAPHITE_SERVER_HOST_PORT, "Graphite server host and port where to report metrics, if set. Port default to 2003 if not set.")
                .withRequiredArg();
        getParser().accepts(OPTION_GRAPHITE_PREFIX, "Prefix to add on the metrics reported to Graphite.")
                .withRequiredArg().defaultsTo(OPTION_GRAPHITE_PREFIX_DEFAULT);
    }


    @Override
    protected void internalClose() throws IOException {
        latch.countDown();
    }

    public boolean isInitialized() {
        return initialized;
    }

    protected class ReturnCode extends AbstractAppLauncher.ReturnCode {

        public static final int ERROR_WITH_DFS_EDIT_DIR = 5;

    }
}
