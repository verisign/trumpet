package com.verisign.vscc.hdfs.trumpet.server.tool;

import com.google.common.base.Preconditions;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.server.editlog.EditLogDir;
import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetServer;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import com.verisign.vscc.hdfs.trumpet.server.rx.EditLogObservable;
import kafka.message.Message;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import rx.Observable;
import rx.Subscriber;
import rx.Subscription;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class EventsValidator extends AbstractAppLauncher {

    private static final String OPTION_DFS_EDITS_DIR = "dfs.edits.dir";

    private static final String OPTION_NUMBER_OF_EVENTS = "numevents";
    private static final int DEFAULT_NUMBER_OF_EVENTS = 1000;

    private final ObjectMapper mapper = new ObjectMapper();

    private File dfsEditsDir;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new EventsValidator(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        int numberOfEvents = Integer.parseInt((String) getOptions().valueOf(OPTION_NUMBER_OF_EVENTS));

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

        final Iterator<Message> it = SimpleConsumerHelper.getNLastMessages(getTopic(), TrumpetServer.PARTITION_NUM, numberOfEvents, getCuratorFrameworkKafka());

        if (!it.hasNext()) {
            System.err.println("No messages found in kafka topic " + getTopic());
            return ReturnCode.NO_MESSAGE_IN_KAFKA;
        }

        EditLogDir editLogDir = new EditLogDir(dfsEditsDir, getConf());

        // note: atomicLong are used here as wrapper around long.
        final AtomicLong lastTxId = new AtomicLong(0);

        final AtomicLong expectedTxId = new AtomicLong(getTxIdFromMessage(it.next()));

        File editsLogFile = editLogDir.searchBestMatchingSegment(expectedTxId.get());

        Long editLogFileStartTxId = EditLogDir.extractStartTxId(editsLogFile);

        // Skip kafka events to match the first found in editLogDir
        while (expectedTxId.get() < editLogFileStartTxId && it.hasNext()) {
            expectedTxId.set(getTxIdFromMessage(it.next()));
        }

        while (it.hasNext()) {

            if (lastTxId.get() == 0) { // first iteration
                lastTxId.set(expectedTxId.get());
            } else {
                lastTxId.incrementAndGet();
            }

            editsLogFile = editLogDir.searchBestMatchingSegment(lastTxId.get());

            // TODO: when lastTxId is too old, the editsLogFile might not contains it.
            System.out.println("Processing " + editsLogFile + " from tx " + lastTxId.get());

            Subscription s = null;
            try {
                s = Observable.create(new EditLogObservable(editsLogFile, lastTxId.get()))
                        .subscribe(new Subscriber<Map<String, Object>>() {
                            boolean ignoreEOF = false;

                            @Override
                            public void onCompleted() {
                                // Good,
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                throwable.printStackTrace();
                                Preconditions.checkState(false, "Got an exception {}", throwable);
                            }

                            @Override
                            public void onNext(Map<String, Object> o) {

                                if (ignoreEOF) {
                                    return;
                                }

                                long txId = ((Number)o.get(EventAndTxId.FIELD_TXID)).longValue();

                                lastTxId.set(txId);
                                if (o.containsKey(EventAndTxId.FIELD_EVENTTYPE)) {

                                    Preconditions.checkState(txId == expectedTxId.get(), "TxIds does not match: from edits log file: " + txId + ", from kafka: " + expectedTxId.get() + ", event is " + o.toString());

                                    if (it.hasNext()) {
                                        expectedTxId.set(getTxIdFromMessage(it.next()));
                                    } else {
                                        // we proceeded all the messages, ignoring the next event.
                                        ignoreEOF = true;
                                    }
                                }
                            }
                        });

            } finally {
                if (s != null) {
                    s.unsubscribe();
                }
            }

        }

        System.out.println("Good, kafka topic " + getTopic() + " and edit.log.dir " + editLogDir + " look consistent.");

        return ReturnCode.ALL_GOOD;
    }

    protected void initParser() {
        getParser().accepts(OPTION_DFS_EDITS_DIR, "Root directory where the edits files are stored " +
                "(i.e. without <serviceId>/current). Mandatory if " +
                DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY + " is not set in /etc/hadoop/conf/hdfs-site.xml.")
                .withRequiredArg();
        getParser().accepts(OPTION_NUMBER_OF_EVENTS, "Maximum number of events to validate")
                .withRequiredArg().defaultsTo(String.valueOf(DEFAULT_NUMBER_OF_EVENTS));
    }

    private long getTxIdFromMessage(Message m) {

        ByteBuffer bb = m.payload().slice();

        try (InputStream in = new ByteBufferInputStream(Collections.singletonList(bb))) {
            JsonNode node = mapper.readTree(in);
            return node.get(EventAndTxId.FIELD_TXID).getNumberValue().longValue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected class ReturnCode extends AbstractAppLauncher.ReturnCode {

        public static final int ERROR_WITH_DFS_EDIT_DIR = 5;
        public static final int NO_MESSAGE_IN_KAFKA = 6;

    }

}
