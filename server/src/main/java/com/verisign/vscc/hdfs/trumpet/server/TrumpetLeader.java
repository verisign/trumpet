package com.verisign.vscc.hdfs.trumpet.server;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import com.verisign.vscc.hdfs.trumpet.server.editlog.EditLogDir;
import com.verisign.vscc.hdfs.trumpet.server.editlog.WatchDog;
import com.verisign.vscc.hdfs.trumpet.server.metrics.Metrics;
import com.verisign.vscc.hdfs.trumpet.server.rx.EditLogObservable;
import com.verisign.vscc.hdfs.trumpet.server.rx.ProducerSubscriber;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.CancelLeadershipException;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TrumpetLeader extends LeaderSelectorListenerAdapter implements LeaderSelectorListener {

    public static final int PARTITION_NUM = 0; // to guarantee inotify event ordering, only one partition is used.

    static final long DEFAULT_BASE_THROTTLE_TIME_MS = 2000;
    private static final long MAX_THROTTLE_TIME_MS = 30000;

    private static Logger LOG = LoggerFactory.getLogger(TrumpetLeader.class);

    private final CuratorFramework curatorFramework;
    private final String topic;
    private final EditLogDir editLogDir;
    private final long baseThrottleTimeMs;
    private final int kafkaRequiredAcks;

    private final DistributedFileSystem dfs;

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();

    private volatile boolean run = true;

    public TrumpetLeader(CuratorFramework curatorFramework, DistributedFileSystem dfs, String topic, EditLogDir editLogDir)
            throws IOException {
        this(curatorFramework, dfs, topic, editLogDir, SimpleConsumerHelper.DEFAULT_REQUIRED_ACKS, DEFAULT_BASE_THROTTLE_TIME_MS);
    }

    public TrumpetLeader(CuratorFramework curatorFramework, DistributedFileSystem dfs, String topic, EditLogDir editLogDir, int kafkaRequiredAcks, long baseThrottleTimeMs)
            throws IOException {
        this.curatorFramework = curatorFramework;
        this.dfs = dfs;
        this.topic = topic;
        this.editLogDir = editLogDir;
        this.kafkaRequiredAcks = kafkaRequiredAcks;
        this.baseThrottleTimeMs = baseThrottleTimeMs;
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        // we are now the leader. This method should not return until we want to relinquish leadership

        Metrics.leadershipUptime();
        Metrics.leadershipCounter().inc();

        LOG.debug("Elected as leader, let's stream!");
        boolean forceResetTxId = false;
        boolean receivedException = false;

        Producer producer = null;

        int fileNotFoundCounter = 0;
        int exceptionCounter = 0;
        int throttleCounter = 0;
        long lastTransactionSeenMs = System.currentTimeMillis();
        File previousEditLogFile = null;
        Exception exception = null;

        try {

            final AtomicLong lastPublishedTxId = getAtomicLong(topic, forceResetTxId, curatorFramework);

            Metrics.lastTransactionId(lastPublishedTxId);

            LOG.debug("Retrieved lastPublishedTxId: {}", lastPublishedTxId.get());

            File editsLogFile = null;
            long startTxId = 0L;

            producer = getProducer(curatorFramework, kafkaRequiredAcks);

            while (run) {

                // startTxId is the next txId we want to handle, inclusive.
                startTxId = lastPublishedTxId.get() + 1;

                try {

                    Timer.Context editLogFileTimer = Metrics.editLogFile().time();

                    try {
                        editsLogFile = editLogDir.searchBestMatchingSegment(startTxId);

                        // Well, filesystem operation is not atomic, we might have had a glitch between
                        // inprogress -> finalized renaming.
                        if (editsLogFile == null) {

                            Metrics.noTxFileFound().mark();
                            fileNotFoundCounter++;

                            LOG.warn("Oups, no editlog file found. Sleeping and then retrying #{}.", fileNotFoundCounter);

                            if (fileNotFoundCounter > 10) {
                                throw new CancelLeadershipException("No file found after " + fileNotFoundCounter + " consecutive retries. Releasing the leadership.");
                            }

                            Thread.sleep(50);
                            continue;
                        }
                    } finally {
                        editLogFileTimer.stop();
                    }

                    LOG.debug("Reading editLog file {} from tx {}", editsLogFile, startTxId);

                    forceResetTxId = false;
                    fileNotFoundCounter = 0;

                    Timer.Context processTimer = Metrics.process().time();

                    Subscription s = null;

                    try {
                        s = Observable.create(getEditLogObservable(editsLogFile, startTxId))
                                .subscribe(getProducerSubscriber(topic, producer, lastPublishedTxId));
                        exceptionCounter = 0;
                    } finally {
                        if (s != null) {
                            s.unsubscribe();
                        }
                        processTimer.stop();
                    }

                    LOG.debug("Read editLog file {} up to tx {}", editsLogFile, lastPublishedTxId.get());

                } catch (CancelLeadershipException e) {
                    receivedException = true;
                    exception = e;
                } catch (Exception e) {
                    LOG.warn("Exception", e);

                    // IOException is most likely safe to receive here,
                    // as it produced by reading the inprogress concurrently the NN/JN writing to it.

                    exceptionCounter++;

                    if (exceptionCounter > 3) {
                        receivedException = true;
                    }

                    exception = e;

                } finally {

                    if (receivedException) {
                        if (exception instanceof CancelLeadershipException) {
                            throw exception;
                        } else {
                            throw new CancelLeadershipException("Got exception. Releasing leadership.", exception);
                        }
                    }

                    // If no editsLogFile found, simply skip the throttling part.
                    if (editsLogFile == null) {
                        continue;
                    }

                    boolean sameFile = editsLogFile.equals(previousEditLogFile);
                    boolean hasNewTx = startTxId <= lastPublishedTxId.get();

                    if (startTxId > lastPublishedTxId.get() || sameFile) {

                        if (hasNewTx) {
                            throttleCounter = 1;
                            lastTransactionSeenMs = System.currentTimeMillis();
                        } else {
                            throttleCounter++;
                        }

                        if (lastTransactionSeenMs < System.currentTimeMillis() - 2 * TimeUnit.SECONDS.toMillis(DFSConfigKeys.DFS_HA_LOGROLL_PERIOD_DEFAULT)) {
                            // no transaction processed for roughly 10 minutes.
                            throw new CancelLeadershipException("No transaction processed for a long time " +
                                    "(throttleCounter=" + throttleCounter +", lastTransactionSeenMs=" + lastTransactionSeenMs + "), cancelling leadership)");
                        }

                        long sleepMs = Math.min(MAX_THROTTLE_TIME_MS,
                                baseThrottleTimeMs * (this.random.nextInt(throttleCounter) + 1));

                        LOG.debug("From tx ({}) > to tx ({}) or sameFile={}, sleeping for {} ms.", startTxId, lastPublishedTxId.get(), sameFile, sleepMs);

                        // Measure sleep time
                        Timer.Context sleepTimer = Metrics.sleep().time();

                        try {
                            // we didn't find any new tx. Throttling...
                            Thread.sleep(sleepMs);
                        } finally {
                            sleepTimer.stop();
                        }

                    } else {
                        throttleCounter = 0;
                    }

                    previousEditLogFile = editsLogFile;

                }
            }


        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }

    private Producer getProducer(CuratorFramework curatorFramework) throws Exception {
        return SimpleConsumerHelper.getProducer(curatorFramework);
    }

    @VisibleForTesting
    public Producer getProducer(CuratorFramework curatorFramework, int requiredAcks) throws Exception {
        return SimpleConsumerHelper.getProducer(curatorFramework, requiredAcks);
    }

    @VisibleForTesting
    public EditLogObservable getEditLogObservable(File editsLogFile, long startTxId) throws Exception {
        return new EditLogObservable(editsLogFile, startTxId);
    }

    @VisibleForTesting
    public ProducerSubscriber getProducerSubscriber(final String topic, final Producer<String, String> producer, final AtomicLong lastSeenTxId) throws Exception {
        return new ProducerSubscriber(topic, producer, lastSeenTxId);
    }

    @VisibleForTesting
    public AtomicLong getAtomicLong(String topic, boolean forceResetTxId, CuratorFramework curatorFramework) throws Exception {
        return new AtomicLong(getLastPublishedTxId(topic, forceResetTxId, curatorFramework));
    }

    private long getLastPublishedTxId(String topic, boolean forceResetTxId, CuratorFramework curatorFramework) throws Exception {

        if (!forceResetTxId) {
            // Read the latest message from the topic (Queue.peak)
            // and get the HDFS txId out of the message.
            // If no message, start from the latest HDFS tx.

            final Message msg = SimpleConsumerHelper.getLastMessage(topic, PARTITION_NUM, curatorFramework);

            String payload = null;

            if (msg != null) {

                final ByteBuffer bb = msg.payload().slice();
                try (InputStream in = new ByteBufferInputStream(Collections.singletonList(bb))) {
                    final JsonNode node = mapper.readTree(in);

                    return node.get(EventAndTxId.FIELD_TXID).getNumberValue().longValue();
                } catch (java.io.CharConversionException | org.codehaus.jackson.JsonParseException e) {
                    LOG.warn("Parsing exception when reading last message from topic. payload is " + payload + ". " +
                            "Re-starting from NN current position", e);
                }

            }
        }

        // if something went wrong in peaking the topic or no previous event published
        // returns the current edit log txId from the namenode - 1 because we want to
        // start at this exact txId, not the next one.
        return dfs.getClient().getNamenode().getCurrentEditLogTxid() - 1;
    }

}
