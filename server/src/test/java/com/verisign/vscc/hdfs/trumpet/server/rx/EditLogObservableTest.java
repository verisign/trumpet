package com.verisign.vscc.hdfs.trumpet.server.rx;

import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.server.editlog.EditLogDir;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.junit.Assert;
import org.junit.Test;
import rx.Observable;
import rx.functions.Action1;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class EditLogObservableTest extends TestCase {

    private static final int BLOCK_SIZE = 1024;

    final static Random r = new Random();

    @Test
    public void testBasic() throws IOException, URISyntaxException,
            InterruptedException, MissingEventsException {
        Configuration conf = new HdfsConfiguration();
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);
        // so that we can get an atime change
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);

        MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
        builder.getDfsBuilder().numDataNodes(2);
        MiniQJMHACluster cluster = builder.build();

        try {
            cluster.getDfsCluster().waitActive();
            cluster.getDfsCluster().transitionToActive(0);

            FileSystem fs = cluster.getDfsCluster().getFileSystem(0);

            int numberOfFiles = r.nextInt(10) + 5;

            for (int i = 0; i < numberOfFiles; i++) {
                DFSTestUtil.createFile(fs, new Path("/file1-" + i), BLOCK_SIZE, (short) 1, 0L);
            }

            long l = cluster.getDfsCluster().getFileSystem(0).getClient().getNamenode().rollEdits();

            for (int i = 0; i < numberOfFiles; i++) {
                DFSTestUtil.createFile(fs, new Path("/file2-" + i), BLOCK_SIZE, (short) 1, 0L);
            }

            final EditLogDir editLogDir = new EditLogDir(new File(cluster.getJournalCluster().getJournalNode(0).getConf().get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY)), fs.getConf());

            File editsLogFile = editLogDir.searchBestMatchingSegment(1L);
            Assert.assertNotNull(editsLogFile);

            final AtomicLong txCounter = new AtomicLong(0L);

            Observable.create(new EditLogObservable(editsLogFile, 1L))
                    .subscribe(new Action1<Map<String, Object>>() {
                        @Override
                        public void call(Map<String, Object> stringObjectMap) {
                            if (stringObjectMap.containsKey(EventAndTxId.FIELD_EVENTTYPE)) {
                                txCounter.incrementAndGet();
                            }
                        }
                    });

            Assert.assertEquals(numberOfFiles * 2, txCounter.get()); // numberOfFiles * 2 means open and close the file.

        } finally {
            cluster.shutdown();
        }
    }
}