package com.verisign.vscc.hdfs.trumpet.kafka;

import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.TestUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class SimpleKafkaTest extends SetupSimpleKafkaCluster {

    private String topic = "test";

    @Test
    public void producerTest() throws Exception {

        Assert.assertNotNull(zkClient);

        // create topic
        AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());

        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);

        // setup producer
        Properties properties = TestUtils.getProducerConfig(kafkaServersToListOfString(servers));

        System.out.println(properties);

        ProducerConfig pConfig = new ProducerConfig(properties);
        Producer producer = new Producer(pConfig);

        // send message
        KeyedMessage<Integer, byte[]> data = new KeyedMessage(topic, "test-message".getBytes());

        List<KeyedMessage> messages = new ArrayList<KeyedMessage>();
        messages.add(data);

        producer.send(messages);
        producer.send(messages);
        producer.send(messages);
        producer.send(messages);
        producer.send(messages);
        producer.send(messages);

        CuratorFramework curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();


        Message lastMessage = SimpleConsumerHelper.getLastMessage(topic, 0, curatorFramework);

        Assert.assertNotNull(lastMessage);
    }

}
