package com.verisign.vscc.hdfs.trumpet.server;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.verisign.vscc.hdfs.trumpet.client.BoundedTrumpetEventStreamer;
import com.verisign.vscc.hdfs.trumpet.client.InfiniteTrumpetEventStreamer;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.kafka.KafkaUtils;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class CompleteIntegrationTest extends IntegrationTest {

    @Test(timeout = 30000L)
    public void test() throws Exception {

        final FileSystem fs = cluster.getDfsCluster().getFileSystem(0);

        final int numberOfFiles = r.nextInt(10) + 5;

        try {
            for (int i = 0; i < numberOfFiles; i++) {
                DFSTestUtil.createFile(fs, new Path("/file2-" + i), BLOCK_SIZE, (short) 1, 0L);
                totalNumberOfFiles++;
            }
        } catch (Exception e) {
            System.err.println("Exception occurred in the file creation process");
            e.printStackTrace();
        }

        Thread.sleep(5000);

        long lastOffset = SimpleConsumerHelper.getLastOffset(curatorFramework, trumpetTopicName, 0);

        Iterator<Map<String, Object>> it = new BoundedTrumpetEventStreamer(curatorFramework, trumpetTopicName, 0L, lastOffset).iterator();

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

        try (InfiniteTrumpetEventStreamer infiniteTrumpetEventStreamer = new InfiniteTrumpetEventStreamer(curatorFramework, trumpetTopicName, lastOffset)) {
            Iterator<Map<String, Object>> it2 = infiniteTrumpetEventStreamer.iterator();

            try {
                for (int j = 0; j < numberOfFiles; j++) {
                    DFSTestUtil.createFile(fs, new Path("/file3-" + j), BLOCK_SIZE, (short) 1, 0L);
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
