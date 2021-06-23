package com.verisign.vscc.hdfs.trumpet.server.rx;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.verisign.vscc.hdfs.trumpet.server.metrics.Metrics;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * EditLog dir watch dog daemon, polling for file modification
 * every 2 minutes (DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT)
 * <p/>
 * If no modification found while new transactions occurs,
 * it closes the parent and shutdown the timer properly.
 */
public class ProducerWatchDog extends TimerTask implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(ProducerWatchDog.class);

    private final AutoCloseable parent;

    private final Timer t = new Timer("Timer for " + ProducerWatchDog.class.getName());

    private long lastSeenTxId = -1L;
    private long lastRunTimeMs = 0L;

    public ProducerWatchDog(AutoCloseable parent) {
        super();
        this.parent = parent;

        schedule();
    }

    @VisibleForTesting
    public void schedule() {
        long time = TimeUnit.SECONDS.toMillis(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        t.scheduleAtFixedRate(this, time, time);
    }


    @Override
    public void run() {

        try {
            Gauge<Long> lastTransactionId = Metrics.lastTransactionId();
            Counter leadershipStatus = Metrics.leadershipStatus();

            long previousLastSeenTxId = lastSeenTxId;

            if (leadershipStatus.getCount() > 0) {
                if (lastTransactionId == null) {
                    // We should wait few ms, the leader is still instantiating...
                }

                lastSeenTxId = lastTransactionId.getValue();

                if (previousLastSeenTxId == -1L || lastSeenTxId > previousLastSeenTxId) {
                    // first run since it's a leader,
                    // or state is ok.
                    return;
                } else {

                    // Since when we didn't see transaction?

//                    LOG.warn(getClass().getSimpleName() + " detected anomaly in the editLogDir, so asking the server to shut down. Details:"
//                            + " mostRecentEditFile=" + mostRecentEditFile + ", lastModified=" + (mostRecentEditFile == null ? "null" : mostRecentEditFile.lastModified())
//                            + ", lastRunTimeMs=" + lastRunTimeMs + ", exceptionCount=" + exceptionCount
//                            + ", currentTxId=" + currentTxId);
                    // last edit log file wasn't updated since last run
                    // even though new transaction have been committed.
                    // This JN/NN is most likely dead and the process
                    // should hand over to another leader.
                    close();
                    parent.close();

                }
            } else {
                // Reinit the state
                lastSeenTxId = -1L;
            }

        } catch (Exception e) {
            LOG.warn("Exception", e);
        } finally {
            lastRunTimeMs = System.currentTimeMillis();
        }
    }

    @Override
    public void close() {
        t.cancel();
        t.purge();
    }
}