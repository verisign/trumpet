package com.verisign.vscc.hdfs.trumpet.kafka;

import com.google.common.base.Joiner;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class KafkaUtilsTest extends SetupSimpleKafkaCluster {

    @Test
    public void testBrokerListDiscovery() throws Exception {

        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);

        assertEquals(kafkaServersString.split(",").length, brokers.size());
        assertEquals(servers.size(), brokers.size());
    }

    @Test
    public void testBrokerListDiscoveryWithNonExistingZKHost() throws Exception {

        CuratorFramework curatorFramework1 = Mockito.mock(CuratorFramework.class);
        CuratorZookeeperClient curatorZookeeperClient = Mockito.mock(CuratorZookeeperClient.class);

        ExistsBuilder existsBuilder = Mockito.mock(ExistsBuilder.class);
        Stat stat = Mockito.mock(Stat.class);

        GetChildrenBuilder childrenBuilder = Mockito.mock(GetChildrenBuilder.class);
        GetDataBuilder dataBuilder = Mockito.mock(GetDataBuilder.class);

        Mockito.when(curatorFramework1.getChildren()).thenReturn(childrenBuilder);
        Mockito.when(childrenBuilder.forPath(eq("/brokers/ids"))).thenReturn(Collections.<String>emptyList());

        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework1);
        assertEquals(0, brokers.size());
    }


    @Test
    public void testTopicCreation() throws Exception {

        final String topic = getClass().getCanonicalName() + "." + (new Object() {
        }.getClass().getEnclosingMethod().getName());

        int replication = Math.min(servers.size(), KafkaUtils.DEFAULT_REPLICATION);

        KafkaUtils.createTopic(topic, KafkaUtils.DEFAULT_NUM_OF_PARTITION, replication, curatorFramework);
        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);

        assertEquals(replication, brokers.size());
        assertTrue(KafkaUtils.topicExists(topic, curatorFramework));
    }

}
