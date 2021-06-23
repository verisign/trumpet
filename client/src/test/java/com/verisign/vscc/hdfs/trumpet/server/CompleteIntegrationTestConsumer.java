package com.verisign.vscc.hdfs.trumpet.server;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.junit.Assert;
import org.junit.Test;

import com.verisign.vscc.hdfs.trumpet.client.BoundedTrumpetEventConsumer;
import com.verisign.vscc.hdfs.trumpet.client.InfiniteTrumpetEventConsumer;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.kafka.ConsumerHelper;

public class CompleteIntegrationTestConsumer extends IntegrationTest {


    @Test(timeout = 150000L)
    public void testNewConsumer() throws Exception {

        final FileSystem fs = cluster.getDfsCluster().getFileSystem(0);

        final int numberOfFiles = r.nextInt(10) + 5;
        Thread.sleep(5000);
        try {
            for (int i = 0; i < numberOfFiles; i++) {
                DFSTestUtil.createFile(fs, new Path("/file4-" + i), BLOCK_SIZE, (short) 1, 0L);
                totalNumberOfFiles++;
            }
        } catch (Exception e) {
            System.err.println("Exception occurred in the file creation process");
            e.printStackTrace();
        }

        Thread.sleep(5000);

        long lastOffset = ConsumerHelper.getLastOffset(curatorFramework, trumpetTopicName, 0, "testIntegration");

        Iterator<Map<String, Object>> it = new BoundedTrumpetEventConsumer(curatorFramework, trumpetTopicName, 0L, lastOffset).iterator();

        int i = 0;
        while (it.hasNext()) {
            Map<String, Object> o = it.next();
            String eventType = (String) o.get(EventAndTxId.FIELD_EVENTTYPE);
            if ("CLOSE".equals(eventType)) {
                i++;
            }
        }

//        numberOfFiles and not totalNumberOfFiles because Trumpet is starting from the current transaction
//        when no message in Kafka.
        Assert.assertEquals(numberOfFiles, i);

        try (InfiniteTrumpetEventConsumer infiniteTrumpetEventConsumer = new InfiniteTrumpetEventConsumer(curatorFramework, trumpetTopicName, lastOffset)) {
            Iterator<Map<String, Object>> it2 = infiniteTrumpetEventConsumer.iterator();

            try {
                for (int j = 0; j < numberOfFiles; j++) {
                    DFSTestUtil.createFile(fs, new Path("/file5-" + j), BLOCK_SIZE, (short) 1, 0L);
                    totalNumberOfFiles++;
                }
            } catch (Exception e) {
                System.err.println("Exception occurred in the file creation process");
                e.printStackTrace();
            }

            Thread.sleep(5000);

            int j = 0;
            while (it2.hasNext()) {
                Map<String, Object> o = it2.next();
                String eventType = (String) o.get(EventAndTxId.FIELD_EVENTTYPE);
                if ("CLOSE".equals(eventType)) {
                    j++;
                }
                if (j >= numberOfFiles) {
                    break;
                }
            }
            Assert.assertEquals(numberOfFiles, j);
        }
        
    }

}
