package com.verisign.vscc.hdfs.trumpet.kafka;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
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
    private static final String TOPIC = "test";

    @Test
    public void producerTest() throws Exception {

        Assert.assertNotNull(zkClient);
        Assert.assertNotNull(zkUtils);

        // create topic
        AdminUtils.createTopic(zkUtils, TOPIC, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        Properties producerProps = new Properties();
        producerProps.setProperty("metadata.broker.list", BROKERHOST + ":" + BROKERPORT);
        producerProps.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producerProps.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        LOG.info(producerProps.toString());

        ProducerConfig pConfig = new ProducerConfig(producerProps);
        Producer producer = new Producer(pConfig);

        // send message
        KeyedMessage<Integer, byte[]> data = new KeyedMessage(topic, "test-message".getBytes());

        List<KeyedMessage> messages = new ArrayList<>();
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
