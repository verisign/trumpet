package com.verisign.vscc.hdfs.trumpet.kafka;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class SimpleKafkaTest extends SetupSimpleKafkaCluster {

    protected final Random r = new Random();

    @Test
    public void producerTest() throws Exception {

        Assert.assertNotNull(curatorFramework);

        String topicName1 = "myTopic";
        int partitions = 1;
        int replication = 1;

        KafkaUtils.createTopic(topicName1, partitions, replication, curatorFramework);


        // setup producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaServersString);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("security.protocol", ConsumerHelper.DEFAULT_SECURITY_PROTOCOL);
        properties.put("acks", "all");

        Producer<String, String> producer = new KafkaProducer(properties);

        // send message
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topicName1, "test-message");

        producer.send(record);
        producer.send(record);
        producer.send(record);
        producer.send(record);
        producer.send(record);
        Future<RecordMetadata> future = producer.send(record);
        

        curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        ConsumerRecord<String, String> lastMessage = ConsumerHelper.getLastRecords(topicName1, 0, curatorFramework);

        Assert.assertNotNull(lastMessage);
    }

}
