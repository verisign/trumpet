package com.verisign.vscc.hdfs.trumpet.kafka;

import static com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher.OPTION_ZK_CONNECT;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Kafka and Consumer helper functions.
 * <p/>
 * Consumer is required here as we want to peak the topic to retrieve the latest
 * published txId, but the API requires some boiler plate which is put here to keep
 * the rest of the code reasonably lean.
 * <p>
 * This is a new version of the SimpleConsumerHelper which is not compatible with security.
 * This class is using the consumer and producer implementation of Kafka 0.10.X.
 */
public class ConsumerHelper {
    private static Logger LOG = LoggerFactory.getLogger(ConsumerHelper.class);

    private static final int DEFAULT_ZK_PORT = 2181;
    private static final int DEFAULT_SO_TIMEOUT_MS = 10000;
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
    public static final String DEFAULT_REQUIRED_ACKS = "1";
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";

    public static String pathConsumerProperties = null;
    public static String pathProducerProperties = null;
    public static Properties consumerProperties = null;
    public static Properties producerProperties = null;
    private static Random R = new Random();

    public static Properties getConsumerProperties() {
        return consumerProperties;
    }

    public static Properties getProducerProperties() {
        return producerProperties;
    }

    public static String getPathProducerProperties() {
        return pathProducerProperties;
    }

    public static void setPathProducerProperties(String pathProducerProperties) {
        ConsumerHelper.pathProducerProperties = pathProducerProperties;
        if (pathProducerProperties != null) {
            producerProperties = KafkaUtils.loadPropertiesFromFile(pathProducerProperties);
        }
    }

    public static String getPathConsumerProperties() {
        return pathConsumerProperties;
    }

    public static void setPathConsumerProperties(String pathConsumerProperties) {
        ConsumerHelper.pathConsumerProperties = pathConsumerProperties;
        if (pathConsumerProperties != null) {
            consumerProperties = KafkaUtils.loadPropertiesFromFile(pathConsumerProperties);
        }
    }
    public static void setConsumerProperties(Properties pop){
        consumerProperties = pop;
    }


    public static long getFirstOffset(KafkaConsumer consumer, String topic, int partition) {
        TopicPartition partitionId = new TopicPartition(topic, partition);
        consumer.seekToBeginning(Arrays.asList(partitionId));
        long offset = consumer.position(partitionId);
        return Math.max(0, offset - 1);
    }


    public static long getLastOffset(KafkaConsumer consumer, String topic, int partition) {
        TopicPartition partitionId = new TopicPartition(topic, partition);
        consumer.seekToEnd(Arrays.asList(partitionId));
        long offset = consumer.position(partitionId);
        return Math.max(0, offset - 1);

    }

    public static KafkaConsumer getConsumer(CuratorFramework curatorFramework, String topic, int[] partitions, String clientName) throws Exception {
        ArrayList<TopicPartition> arrayTopicPartition = new ArrayList<>();
        for (int i : partitions) {
            TopicPartition partitionId = new TopicPartition(topic, i);
            arrayTopicPartition.add(partitionId);
        }
        return getConsumer(curatorFramework, topic, arrayTopicPartition, clientName);
    }

    public static KafkaConsumer getConsumer(CuratorFramework curatorFramework, String topic, Collection<TopicPartition> partitions, String clientName) throws Exception {
        String clientId = "Trumpet_" + clientName + "_" + topic + "_manyPartition_" + partitions.toArray()[0];
        KafkaConsumer consumer = getConsumerForTopic(curatorFramework, topic, clientId);
        return consumer;
    }

    public static KafkaConsumer getConsumer(CuratorFramework curatorFramework, String topic, int partition, String clientName) throws Exception {
        String clientId = "Trumpet_" + clientName + "_" + topic + "_" + partition;
        KafkaConsumer consumer = getConsumerForTopicPartition(curatorFramework, topic, partition, clientId);

        return consumer;
    }

    public static long getLastOffset(CuratorFramework curatorFramework, String topic, int partition, String clientName) throws Exception {
        String clientId = "Trumpet_" + clientName + "_" + topic;
        KafkaConsumer consumer = getConsumerForTopicPartition(curatorFramework, topic, partition, clientId);
        TopicPartition partitionId = new TopicPartition(topic, partition);
        long offset = consumer.position(partitionId);

        consumer.close();
        return Math.max(0, offset - 1);
    }

    public static Iterator<ConsumerRecord<String, String>> getNLastMessages(final String topic, final int partition, final int numberOfMessages, CuratorFramework curatorFramework) throws Exception {
        String clientId = "Trumpet_getOffset_" + topic + "_" + partition;
        KafkaConsumer consumer = getConsumerForTopicPartition(curatorFramework, topic, partition, clientId);
        TopicPartition partitionId = new TopicPartition(topic, partition);
        long offset = consumer.position(partitionId);
        LOG.debug("Last offset is {}", offset);
        consumer.close();

        return getMessagesFromTo(topic, partition, offset - numberOfMessages, offset, "readNlastMsg", curatorFramework);
    }

    private static ConsumerRecord<String, String> internalReadMessageAtOffset(String topic, int partition, long readOffset, String clientName, KafkaConsumer consumer) {
        LOG.trace("internal read topic {} at offset {}", topic, readOffset);
        TopicPartition partitionId = new TopicPartition(topic, partition);

        long lastOffset = consumer.position(partitionId);
        LOG.debug("Last offset is {}", lastOffset);
        if (lastOffset < readOffset)
            LOG.warn("cannot read an offset in the futur. Max offset for topic {} and partition {} is {}. Asking for {}", topic, partition, lastOffset, readOffset);
        consumer.seek(partitionId, readOffset);

        Duration d = Duration.ofMillis(100);
        ConsumerRecords<String, String> records = consumer.poll(d);
        if (records.isEmpty()) {
            LOG.info("Found no records for topic {} and partition {}. Asking for offset {}", topic, partition, readOffset);
        }

        ConsumerRecord<String, String> returnRecord = null;
        for (ConsumerRecord<String, String> record : records) {
            if (record.offset() == readOffset) {
                returnRecord = record;
                break;
            }

        }
        return returnRecord;

    }

    public static ConsumerRecord<String, String> getLastRecords(String topic, int partition, CuratorFramework curatorFramework) throws Exception {
        String clientId = "Trumpet_" + topic;
        KafkaConsumer consumer = getConsumerForTopicPartition(curatorFramework, topic, partition, clientId);

        TopicPartition partitionId = new TopicPartition(topic, partition);
        consumer.seekToEnd(Arrays.asList(partitionId));
        long offset = consumer.position(partitionId);
        LOG.debug("Last offset is {}", offset);

        // Rewind offset by 1.
        offset = Math.max(0, offset - 1);
        consumer.seek(partitionId, offset);
        Duration d = Duration.ofMillis(100);
        ConsumerRecords<String, String> records = consumer.poll(d);
        if (records.isEmpty()) {
            LOG.info("C : {}, Found no records", clientId);
        }

        ConsumerRecord<String, String> returnRecord = null;
        for (ConsumerRecord<String, String> record : records) {
            returnRecord = record;
            if (records.count() > 1)
                break;
        }

        consumer.close();

        return returnRecord;

    }


    public static Producer<String, String> getProducer(CuratorFramework curatorFramework) throws Exception {
        Properties props = getProducerProperties();

        if (props == null) {
            props = new Properties();
        }
        if (props.getProperty("security.protocol") == null || props.getProperty("security.protocol").isEmpty()) {
            props.put("security.protocol", DEFAULT_SECURITY_PROTOCOL);
        }
        if (props.getProperty("bootstrap.servers") == null || props.getProperty("bootstrap.servers").isEmpty()) {
            List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
            Preconditions.checkState(brokers.size() > 0);
            props.put("bootstrap.servers", Joiner.on(",").join(brokers));
        }
        if (props.getProperty("key.serializer") == null || props.getProperty("key.serializer").isEmpty()) {
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }
        if (props.getProperty("value.serializer") == null || props.getProperty("value.serializer").isEmpty()) {
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }
        if (props.getProperty("acks") == null || props.getProperty("acks").isEmpty()) {
            props.put("acks", DEFAULT_REQUIRED_ACKS);
        }


        LOG.info("Getting a new producer with the following properties");
        LOG.debug("producer properties{}", props);

        Producer<String, String> producer = new KafkaProducer<>(props);

        return producer;
    }

    public static void send(Producer<String, String> producer, String topic, String key, String value) {
        ProducerRecord<String, String> data = new ProducerRecord<String, String>(topic, key, value);
        producer.send(data);
    }


    public static KafkaConsumer getConsumerForTopic(CuratorFramework curatorFramework, String topic, String groupId) throws Exception {
        Properties props = createConsumerConfig(curatorFramework, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static KafkaConsumer getConsumerForTopicPartition(CuratorFramework curatorFramework, String topic, int partition, String groupId) throws Exception {
        Properties props = createConsumerConfig(curatorFramework, groupId);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition partitionId = new TopicPartition(topic, partition);
        consumer.assign(Arrays.asList(partitionId));
        return consumer;
    }

    public static Properties createConsumerConfig(CuratorFramework curatorFramework) throws Exception {
        return createConsumerConfig(curatorFramework, "Trumpet_consumer");
    }
    public static Properties createConsumerConfig(CuratorFramework curatorFramework, String groupId) throws Exception {
        Properties props = getConsumerProperties();

        if (props == null) {
            props = new Properties();
        }
        if (props.getProperty("bootstrap.servers") == null || props.getProperty("bootstrap.servers").isEmpty()) {
            List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
            Preconditions.checkState(brokers.size() > 0);
            props.put("bootstrap.servers", Joiner.on(",").join(brokers));
        }
        if (props.getProperty("key.deserializer") == null || props.getProperty("key.deserializer").isEmpty()) {
            props.put("key.deserializer", StringDeserializer.class);
        }
        if (props.getProperty("value.deserializer") == null || props.getProperty("value.deserializer").isEmpty()) {
            props.put("value.deserializer", StringDeserializer.class);
        }
        if (props.getProperty("group.id") == null || props.getProperty("group.id").isEmpty()) {
            props.put("group.id", groupId);
        }
        if (props.getProperty("security.protocol") == null || props.getProperty("security.protocol").isEmpty()) {
            props.put("security.protocol", DEFAULT_SECURITY_PROTOCOL);
        }

        return props;
    }

    public static Iterator<ConsumerRecord<String, String>> getMessagesFrom(final String topic, final int partition, final long firstOffset, CuratorFramework curatorFramework) throws Exception {
        return getMessagesFromTo(topic, partition, firstOffset, null, "Trumpet_" + topic + "-" + partition + "-" + firstOffset, curatorFramework);
    }

    public static Iterator<ConsumerRecord<String, String>> getMessagesFrom(final String topic, final int partition, final long firstOffset, String clientId, CuratorFramework curatorFramework) throws Exception {
        return getMessagesFromTo(topic, partition, firstOffset, null, clientId, curatorFramework);
    }

    public static Iterator<ConsumerRecord<String, String>> getMessagesFromTo(final String topic, final int partition, final long firstOffset, final Long lastOffset, CuratorFramework curatorFramework) throws Exception {
        return getMessagesFromTo(topic, partition, firstOffset, lastOffset, "Trumpet_" + topic + "-" + partition + "-" + firstOffset + "-" + lastOffset, curatorFramework);
    }

    public static Iterator<ConsumerRecord<String, String>> getMessagesFromTo(final String topic, final int partition, final long firstOffset, final Long lastOffset, final String clientName, final CuratorFramework curatorFramework) throws Exception {

        /* Offset management are a bit different with the new consumer API. When we commitSync, we commit the next offset:
         * "The committed offset should be the next message your application will consume" : https://kafka.apache.org/11/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
         * This mean when we "seek" to a specfific offset it should be the one we want to consume and not the last one we consume.
         *  "currentOffset" var will maintain the nextOffsetToRead. This is why we are adding 1 to the message.offset.
         *  When currentOffset = lastOffset, we still need to read that message. So, if in arg firstOffset = lastOffset, there is one message to read.
         */
        return new Iterator<ConsumerRecord<String, String>>() {
            long currentOffset = firstOffset;
            boolean more = true;
            TopicPartition partitionId = new TopicPartition(topic, partition);

            KafkaConsumer consumer;
            Iterator<ConsumerRecord<String, String>> iterator;
            int numErrors = 0;

            @Override
            public boolean hasNext() {
                fetch();
                boolean r = more && (lastOffset == null || currentOffset <= lastOffset) && iterator.hasNext();

                if (!r) {
                    if (consumer != null) {
                        consumer.close();
                    }
                }

                return r;
            }

            @Override
            public void remove() {
                throw new IllegalStateException("Unimplemented -- this is a read-only iterator");
            }


            @Override
            public ConsumerRecord<String, String> next() {

                try {
                    fetch();
                    ConsumerRecord m = iterator.next();
                    currentOffset = m.offset()+1; //m.nextOffset();
                    return m;
                } finally {

                }
            }

            private void fetch() {

                if (consumer == null) {
                    initConsumer();
                }

                if (iterator == null || !iterator.hasNext()) {

                    do {
                        consumer.seek(partitionId, currentOffset);
                        try {
                            Duration d = Duration.ofSeconds(1);
                            ConsumerRecords<String, String> records = consumer.poll(d);
                            iterator = records.iterator();
                            for (TopicPartition partition : records.partitions()) {
                                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                                long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
                            }
                        } catch (InvalidOffsetException ioe) {
                            numErrors++;
                            LOG.error("Error fetching data, invalid offset Exception. Reason: " + ioe.getMessage());

                        } catch (AuthorizationException ae) {
                            numErrors++;
                            LOG.error("Error fetching data Authorization exception Reason: " + ae.getMessage());

                        } catch (KafkaException ke) {
                            numErrors++;
                            LOG.error("Error fetching data from Kafka. KafkaException, Reason: " + ke.getMessage());

                        } finally {
                            if (numErrors > 5) {
                                consumer.close();
                                throw new RuntimeException("Too many errors in a row. Aborting");

                            }
                        }

                        numErrors = 0;
                        break;
                    } while (true);
                }
            }

            private void initConsumer() {
                try {
                    LOG.debug(OPTION_ZK_CONNECT);
                    List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
                    Preconditions.checkState(brokers.size() > 0);
                    String groupId = "Trumpet_" + clientName + "_" + topic + "_" + partition;
                    Properties props = createConsumerConfig(curatorFramework, groupId);
                    props.put("enable.auto.commit", false);
                    consumer = new KafkaConsumer<>(props);
                    TopicPartition partitionId = new TopicPartition(topic, partition);
                    consumer.assign(Arrays.asList(partitionId));

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        };
    }
}
