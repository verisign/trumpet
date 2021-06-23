package com.verisign.vscc.hdfs.trumpet.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.kafka.clients.admin.AdminClient;
import org.junit.Test;

import kafka.zk.EmbeddedZookeeper;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class KafkaUtilsTest extends SetupSimpleKafkaCluster {
    protected String zkConnect;
    protected EmbeddedZookeeper zkServer;
    private int brokerId = 0;


    @Test
    public void testBrokerListDiscovery() throws Exception {

        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
        assertEquals(kafkaServersString.split(",").length, brokers.size());
        assertEquals(servers.size(), brokers.size());
    }

    @Test
    public void testTopicCreation() throws Exception {

        final String topic = getClass().getCanonicalName() + "." + (new Object() {
        }.getClass().getEnclosingMethod().getName());

        int replication = Math.min(servers.size(), KafkaUtils.DEFAULT_REPLICATION);

        AdminClient adminClient = AdminClient.create(ConsumerHelper.createConsumerConfig(curatorFramework));

        KafkaUtils.createTopic(topic, 10, replication, curatorFramework);
        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);



        assertEquals(replication, brokers.size());
        assertTrue(KafkaUtils.topicExists(topic, curatorFramework));
    }

}
