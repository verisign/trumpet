package com.verisign.vscc.hdfs.trumpet.kafka;

import com.google.common.base.Preconditions;
import com.verisign.vscc.hdfs.trumpet.utils.TrumpetHelper;
import kafka.admin.AdminUtils;
import kafka.message.Message;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Kafka utility functions.
 * <p/>
 * The rational is to have only a dependency on Zookeeper
 * which we have anyway for the leader election
 * and then deduce the broker list from ZK.
 */
public class KafkaUtils {

    public static final int DEFAULT_NUM_OF_PARTITION = 1;
    public static final int DEFAULT_REPLICATION = 3;

    private static ZkClient fromCurator(CuratorFramework curatorFramework) {
        ZkClient zkClient1 = new ZkClient(curatorFramework.getZookeeperClient().getCurrentConnectionString(), 10000, 10000);
        zkClient1.waitUntilConnected();
        return zkClient1;
    }

    public static List<String> retrieveBrokerListFromZK(final CuratorFramework curatorFramework) throws Exception {
        final List<String> brokers = new LinkedList<>();
        List<String> znodes = curatorFramework.getChildren().forPath("/brokers/ids");
        for (String znode : znodes) {
            Map<String, Object> stringObjectMap = TrumpetHelper.toMap(curatorFramework.getData().forPath("/brokers/ids/" + znode));
            brokers.add(stringObjectMap.get("host").toString() + ":" + stringObjectMap.get("port"));
        }
        return brokers;
    }

    public static void createTopic(String topic, CuratorFramework curatorFramework) {
        createTopic(topic, DEFAULT_NUM_OF_PARTITION, DEFAULT_REPLICATION, curatorFramework);
    }

    public static void createTopic(String topic, int partitions, int replication, CuratorFramework curatorFramework) {
        Preconditions.checkArgument(partitions > 0);
        Preconditions.checkArgument(replication > 0);
        ZkClient zkClient = fromCurator(curatorFramework);

        try {
            AdminUtils.createTopic(zkClient, topic, partitions, replication, new Properties());
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static boolean topicExists(String topic, CuratorFramework curatorFramework) {
        ZkClient zkClient = fromCurator(curatorFramework);
        try {
            zkClient = fromCurator(curatorFramework);
            return AdminUtils.topicExists(zkClient, topic);
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }

    public static byte[] toByteArray(Message m) {
        ByteBuffer buf = m.payload();
        byte[] dst = new byte[buf.limit()];
        buf.get(dst);
        return dst;
    }
}
