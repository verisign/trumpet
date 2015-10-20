package com.verisign.vscc.hdfs.trumpet.server.editlog;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
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
public class WatchDog extends TimerTask implements Closeable {

    private static Logger LOG = LoggerFactory.getLogger(WatchDog.class);

    private final DistributedFileSystem dfs;
    private final EditLogDir editLogDir;
    private final AutoCloseable parent;

    private final Timer t = new Timer("Timer for " + WatchDog.class.getName());

    private long lastSeenTxId = -1L;
    private long lastRunTimeMs = 0L;
    private long exceptionCount = 0;

    public WatchDog(DistributedFileSystem dfs, EditLogDir editLogDir, AutoCloseable parent) {
        super();
        this.dfs = dfs;
        this.editLogDir = editLogDir;
        this.parent = parent;

        schedule();
    }

    @VisibleForTesting
    public void schedule() {
        long time = TimeUnit.SECONDS.toMillis(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT);
        t.scheduleAtFixedRate(this, time, time);
    }

    @VisibleForTesting
    public long getLastSeenTxId() {
        return lastSeenTxId;
    }

    @VisibleForTesting
    public long getLastRunTimeMs() {
        return lastRunTimeMs;
    }

    @Override
    public void run() {

        try {
            long currentTxId = dfs.getClient().getNamenode().getCurrentEditLogTxid();
            long previousLastSeenTxId = lastSeenTxId;
            lastSeenTxId = currentTxId;

            if (previousLastSeenTxId == -1L || currentTxId <= previousLastSeenTxId) {
                // first run, just checking that the modification time to
                // or low activity period, nothing to worry about.
                return;
            } else {

                File mostRecentEditFile = editLogDir.searchBestMatchingSegment(currentTxId);

                if (mostRecentEditFile == null || mostRecentEditFile.lastModified() < lastRunTimeMs) {

                    LOG.warn(getClass().getSimpleName() + " detected anomaly in the editLogDir, so asking the server to shut down. Details:"
                    + " mostRecentEditFile=" + mostRecentEditFile + ", lastModified=" + (mostRecentEditFile == null ? "null" : mostRecentEditFile.lastModified())
                    + ", lastRunTimeMs=" + lastRunTimeMs + ", exceptionCount=" + exceptionCount
                    + ", currentTxId=" + currentTxId);
                    // last edit log file wasn't updated since last run
                    // even though new transaction have been committed.
                    // This JN/NN is most likely dead and the process
                    // should hand over to another leader.
                    close();
                    parent.close();
                }

                // the run succeeded, reseting the exception counter.
                exceptionCount = 0;
            }

        } catch (Exception e) {
            LOG.warn("Exception", e);
            exceptionCount++;

            if (exceptionCount > 3) {
                close();
                try {
                    parent.close();
                } catch (Exception e1) {
                    // Can't recover from this state. The entire application will be shutdown anyway.
                    LOG.warn("Exception1", e1);
                }
            }
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
