package com.verisign.vscc.hdfs.trumpet.kafka;

import com.google.common.base.Preconditions;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.utils.TestUtils;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Properties;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class SimpleConsumerHelperTest extends SetupSimpleKafkaCluster {

    @Test
    public void test() throws Exception {

        final String topic = "topic123";
        final String key = "1234";
        final String value = "value1234";
        final int partition = 0;

        if (!KafkaUtils.topicExists(topic, curatorFramework)) {
            AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
            TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
        }

        Preconditions.checkState(KafkaUtils.topicExists(topic, curatorFramework));

        Producer<String, String> producer = SimpleConsumerHelper.getProducer(curatorFramework);

        SimpleConsumerHelper.send(producer, topic, key, value);

        Thread.sleep(1000);

        Message message = SimpleConsumerHelper.getLastMessage(topic, partition, curatorFramework);

        Assert.assertNotNull(message);
        ByteBuffer keyBB = message.key();
        byte[] keyba = new byte[keyBB.limit()];
        keyBB.get(keyba);

        System.out.println("Key:" + new String(keyba));
        Assert.assertArrayEquals(key.getBytes(), keyba);

        ByteBuffer payloadBB = message.payload();
        byte[] payloadba = new byte[payloadBB.limit()];
        payloadBB.get(payloadba);

        Assert.assertArrayEquals(value.getBytes(), payloadba);
    }


    @Test
    public void test2() throws Exception {

        final String topic = "topic123";
        final String key = "1234";
        final String value = "value1234";
        final int partition = 0;

        if (!KafkaUtils.topicExists(topic, curatorFramework)) {
            AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
            TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 5000);
        }

        Preconditions.checkState(KafkaUtils.topicExists(topic, curatorFramework));

        Producer<String, String> producer = SimpleConsumerHelper.getProducer(curatorFramework);

        for (int i = 0; i < 10000; i++) {
            SimpleConsumerHelper.send(producer, topic, key + "-" + i, value + "-" + i);
        }

        Thread.sleep(1000);

        Iterator<Message> it = SimpleConsumerHelper.getMessagesFromTo(topic, partition, 12, 2500L, curatorFramework);

        for (int i = 12; i <= 2500; i++) {

            Assert.assertTrue(it.hasNext());

            Message message = it.next();

            ByteBuffer payloadBB = message.payload();
            byte[] payloadba = new byte[payloadBB.limit()];
            payloadBB.get(payloadba);

            Assert.assertEquals(value + "-" + i, new String(payloadba));
        }

        Assert.assertFalse(it.hasNext());


        it = SimpleConsumerHelper.getMessagesFromTo(topic, partition, 0, 0L, curatorFramework);
        Assert.assertFalse(it.hasNext());

        it = SimpleConsumerHelper.getMessagesFromTo(topic, partition, 2500, 12L, curatorFramework);
        Assert.assertFalse(it.hasNext());

    }



}
