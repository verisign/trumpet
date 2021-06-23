package com.verisign.vscc.hdfs.trumpet.kafka;

import java.util.Iterator;
import java.util.Random;

import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Preconditions;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class ConsumerHelperTest extends SetupSimpleKafkaCluster {
    protected final Random r = new Random();
    @Test
    public void test() throws Exception {

        final String topic = "topic123";
        final String key = "123";
        final String value = "value123";
        final int partition = 0;
        if (!KafkaUtils.topicExists(topic, curatorFramework)) {
            AdminClient adminClient = AdminClient.create(ConsumerHelper.createConsumerConfig(curatorFramework));
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            try {
                final CreateTopicsResult result = adminClient.createTopics(ImmutableList.of(newTopic));
                result.all().get();
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create topic:" + topic, e);
            }
    }

        Preconditions.checkState(KafkaUtils.topicExists(topic, curatorFramework));

        Producer<String, String> producer = ConsumerHelper.getProducer(curatorFramework);

        ConsumerHelper.send(producer, topic, key, value);

        Thread.sleep(3000);

        ConsumerRecord<String, String> record = ConsumerHelper.getLastRecords(topic, partition, curatorFramework);

        Assert.assertNotNull(record);
        String recKey = record.key();

        System.out.println("Key:" + recKey);
        Assert.assertTrue(key.equals(recKey));

        String recValue = record.value();

        Assert.assertTrue(value.equals(recValue));
    }




    @Test
    public void test2() throws Exception {

        final String topic = "topic789";
        final String key = "789";
        final String value = "value789";
        final int partition = 0;

        if (!KafkaUtils.topicExists(topic, curatorFramework)) {
            AdminClient adminClient = AdminClient.create(ConsumerHelper.createConsumerConfig(curatorFramework));
            NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
            try {
                final CreateTopicsResult result = adminClient.createTopics(ImmutableList.of(newTopic));
                result.all().get();
            } catch (final Exception e) {
                throw new RuntimeException("Failed to create topic:" + topic, e);
            }
    }
        Preconditions.checkState(KafkaUtils.topicExists(topic, curatorFramework));

        Producer<String, String> producer = ConsumerHelper.getProducer(curatorFramework);

        for (int i = 0; i < 10000; i++) {
            ConsumerHelper.send(producer, topic, key + "-" + i, value + "-" + i);
        }

        Thread.sleep(3000);

        Iterator<ConsumerRecord<String, String>> it = ConsumerHelper.getMessagesFromTo(topic, partition, 12, 2500L, curatorFramework);

        for (int i = 12; i <= 2500; i++) {

            Assert.assertTrue(it.hasNext());

            ConsumerRecord<String, String> record = it.next();
            String val = record.value();

            Assert.assertEquals(value + "-" + i, val);
        }

        Assert.assertFalse(it.hasNext());


        it = ConsumerHelper.getMessagesFromTo(topic, partition, 0, 0L, curatorFramework);
        Assert.assertTrue(it.hasNext());

        it = ConsumerHelper.getMessagesFromTo(topic, partition, 2500, 12L, curatorFramework);
        Assert.assertFalse(it.hasNext());

    }

    @Test
    public void test3() throws Exception {

        final String topic = "topic456";
        final String key = "456";
        final String value = "value456";
        final int partition = 0;
       
            if (!KafkaUtils.topicExists(topic, curatorFramework)) {
                AdminClient adminClient = AdminClient.create(ConsumerHelper.createConsumerConfig(curatorFramework));
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                try {
                    final CreateTopicsResult result = adminClient.createTopics(ImmutableList.of(newTopic));
                    result.all().get();
                } catch (final Exception e) {
                    throw new RuntimeException("Failed to create topic:" + topic, e);
                }
        }

        Preconditions.checkState(KafkaUtils.topicExists(topic, curatorFramework));
        ConsumerHelper.setPathProducerProperties("producer.properties");
        Producer<String, String> producer = ConsumerHelper.getProducer(curatorFramework);

        ConsumerHelper.send(producer, topic, key, value);

        Thread.sleep(1000);

        ConsumerRecord<String, String> record = ConsumerHelper.getLastRecords(topic, partition, curatorFramework);

        Assert.assertNotNull(record);
        String recKey = record.key();

        System.out.println("Key:" + recKey);
        Assert.assertTrue(key.equals(recKey));

        String recValue = record.value();

        Assert.assertTrue(value.equals(recValue));
    }



}
