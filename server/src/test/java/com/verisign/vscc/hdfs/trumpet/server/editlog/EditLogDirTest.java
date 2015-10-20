package com.verisign.vscc.hdfs.trumpet.server.editlog;

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

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

/**
 * Created by bperroud on 8/15/15.
 */
public class EditLogDirTest extends TestCase {

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

            long l1 = cluster.getDfsCluster().getFileSystem(0).getClient().getNamenode().rollEdits();

            for (int i = 0; i < numberOfFiles; i++) {
                DFSTestUtil.createFile(fs, new Path("/file2-" + i), BLOCK_SIZE, (short) 1, 0L);
            }

            final EditLogDir editLogDir = new EditLogDir(new File(cluster.getJournalCluster().getJournalNode(0).getConf().get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY)), fs.getConf());

            File editLog = editLogDir.searchBestMatchingSegment(l1);
            Assert.assertTrue(EditLogDir.isInProgress(editLog));

            File editLog1L = editLogDir.searchBestMatchingSegment(1L);
            Assert.assertTrue(!EditLogDir.isInProgress(editLog1L));

            File editLoglMinus1 = editLogDir.searchBestMatchingSegment(l1 - 1);
            Assert.assertTrue(!EditLogDir.isInProgress(editLoglMinus1));

            File editLoglPlus1 = editLogDir.searchBestMatchingSegment(l1 + 1);
            Assert.assertTrue(EditLogDir.isInProgress(editLoglPlus1));

            long l2 = cluster.getDfsCluster().getFileSystem(0).getClient().getNamenode().rollEdits();

            File editLogl2 = editLogDir.searchBestMatchingSegment(l2);
            Assert.assertTrue(EditLogDir.isInProgress(editLogl2));

            File editLoglOneMoreTime = editLogDir.searchBestMatchingSegment(l1);
            Assert.assertTrue(!EditLogDir.isInProgress(editLoglOneMoreTime));

        } finally {
            cluster.shutdown();
        }
    }
}