package com.verisign.vscc.hdfs.trumpet.server;

import com.google.common.base.Preconditions;
import com.verisign.vscc.hdfs.trumpet.server.editlog.EditLogDir;
import com.verisign.vscc.hdfs.trumpet.server.editlog.WatchDog;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class TrumpetServer implements Runnable, AutoCloseable {

    public static final int PARTITION_NUM = 0; // to guarantee inotify event ordering, only one partition is used.

    static final long DEFAULT_BASE_THROTTLE_TIME_MS = 2000;
    private static final long MAX_THROTTLE_TIME_MS = 30000;

    private static Logger LOG = LoggerFactory.getLogger(TrumpetServer.class);

    private final CuratorFramework curatorFramework;
    private final Configuration conf;
    private final String topic;
    private final File dfsEditsDir;
    private final long baseThrottleTimeMs;

    private final DistributedFileSystem dfs;
    private final LeaderSelector leaderSelector;
    private final TrumpetLeader trumpetLeader;
    private final EditLogDir editLogDir;
    private final WatchDog watchDog;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();

    private volatile boolean run = true;
    private final CountDownLatch executionLatch = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TrumpetServer(CuratorFramework curatorFramework, Configuration conf, String topic, File dfsEditsDir)
            throws IOException {
        this(curatorFramework, conf, topic, dfsEditsDir, DEFAULT_BASE_THROTTLE_TIME_MS);
    }

    public TrumpetServer(CuratorFramework curatorFramework, Configuration conf, String topic, File dfsEditsDir, long baseThrottleTimeMs)
            throws IOException {
        this.curatorFramework = curatorFramework;
        this.conf = conf;
        this.topic = topic;
        this.dfsEditsDir = dfsEditsDir;
        this.baseThrottleTimeMs = baseThrottleTimeMs;

        FileSystem fs = FileSystem.get(conf);
        Preconditions.checkState(fs instanceof DistributedFileSystem, "FileSystem is not a DistributedFileSystem");
        dfs = (DistributedFileSystem) fs;

        editLogDir = new EditLogDir(dfsEditsDir, conf);
        watchDog = new WatchDog(dfs, editLogDir, this);

        trumpetLeader = new TrumpetLeader(curatorFramework, dfs, topic, editLogDir, baseThrottleTimeMs);

        leaderSelector = new LeaderSelector(curatorFramework, zkLeaderElectionName(topic) , trumpetLeader);
        leaderSelector.autoRequeue();
    }


    @Override
    public void run() {
        // Starting ZK leader election. Once elected, TrumpetLeader class is taking over.
        leaderSelector.start();
        try {
            executionLatch.await();
        } catch (InterruptedException e) {
            try {
                close();
            } catch (IOException e1) {
                LOG.warn("Got IOException while shutting down", e1);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            leaderSelector.interruptLeadership();
            leaderSelector.close();
            executionLatch.countDown();
            watchDog.close();
        }
    }

    public static String zkLeaderElectionName(String topic) {
        return "/" + TrumpetServer.class.getCanonicalName().replace(".", "/") + "/" + topic;
    }

}
