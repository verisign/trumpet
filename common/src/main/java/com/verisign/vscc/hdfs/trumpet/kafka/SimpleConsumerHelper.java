package com.verisign.vscc.hdfs.trumpet.kafka;


import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.LeaderNotAvailableException;
import kafka.common.TopicAndPartition;
import kafka.common.UnknownTopicOrPartitionException;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Kafka and SimpleConsumer helper functions.
 * <p/>
 * SimpleConsumer is required here as we want to peak the topic to retrieve the latest
 * published txId, but the API requires some boiler plate which is put here to keep
 * the rest of the code reasonably lean.
 *
 * @see https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
 * @see https://gist.github.com/ashrithr/5811266
 */
public class SimpleConsumerHelper {

    private static Logger LOG = LoggerFactory.getLogger(SimpleConsumerHelper.class);

    private static final int DEFAULT_ZK_PORT = 2181;
    private static final int DEFAULT_SO_TIMEOUT_MS = 10000;
    private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    private static long[] getLatestOffsets(SimpleConsumer consumer, String topic, int partition,
                                           long whichTime, String clientName, int numberOfOffests) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, numberOfOffests));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            LOG.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return null;
        }
        long[] offsets = response.offsets(topic, partition);

        return offsets;

    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                      long whichTime, String clientName) {
        long[] offsets = getLatestOffsets(consumer, topic, partition, whichTime, clientName, 1);
        if (offsets == null) {
            return 0;
        } else {
            if (offsets.length > 0) {
                return offsets[0];
            } else {
                return 0;
            }
        }
    }

    public static long getLastOffset(CuratorFramework curatorFramework, String topic, int partition) throws Exception {
        String clientId = "GetLastOffsetFor" + topic + "-" + partition;
        SimpleConsumer c = SimpleConsumerHelper.getSimpleConsumerForTopicAndPartition(curatorFramework, topic, partition, clientId);
        try {
            return SimpleConsumerHelper.getLastOffset(c, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientId);
        } finally {
            c.close();
        }
    }

    public static Iterator<Message> getNLastMessages(final String topic, final int partition, final int numberOfMessages, CuratorFramework curatorFramework) throws Exception {

        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);

        PartitionMetadata metadata = findLeader(brokers, topic, partition);
        if (metadata == null) {
            LOG.error("Can't find metadata for Topic and Partition. Exiting");
            return null;
        }
        if (metadata.leader() == null) {
            LOG.error("Can't find Leader for Topic and Partition. Exiting");
            return null;
        }
        String leadBrokerHost = metadata.leader().host();
        int leadBrokerPort = metadata.leader().port();
        final String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer = null;
        long[] readOffsets = null;
        try {
            consumer = new SimpleConsumer(leadBrokerHost, leadBrokerPort, DEFAULT_SO_TIMEOUT_MS, DEFAULT_BUFFER_SIZE, clientName);
            readOffsets = getLatestOffsets(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName, numberOfMessages);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }

        LOG.debug("Latest offsets are {}", Arrays.toString(readOffsets));

        if (readOffsets == null) {
            return null;
        }

        final long lastOffset = readOffsets[0];

        return getMessagesFromTo(topic, partition, lastOffset - numberOfMessages, lastOffset, clientName, curatorFramework);
    }

    private static Message internalReadMessageAtOffset(String topic, int partition, long readOffset, String clientName, SimpleConsumer consumer) {

        LOG.trace("internal read topic {} at offset {}", topic, readOffset);
        FetchRequest req = new FetchRequestBuilder()
                .clientId(clientName)
                .addFetch(topic, partition, readOffset, DEFAULT_BUFFER_SIZE)
                .build();

        FetchResponse fetchResponse = consumer.fetch(req);
        if (fetchResponse.hasError()) {

            short code = fetchResponse.errorCode(topic, partition);
            LOG.error("Error fetching data from the Broker. " + consumer.host() + ":" + consumer.port() + " Reason: " + code);
            //                if (numErrors > 5) break;
            //                if (code == ErrorMapping.OffsetOutOfRangeCode())  {
            //                      // We asked for an invalid offset. For simple case ask for the last element to reset
            //                      readOffset = getLastOffset(consumer,a_topic, a_partition, kafka.api.OffsetRequest.LatestTime(), clientName);
            //                      continue;
            //                  }
            //                consumer.close();
            //                consumer = null;
            //                leadBroker = findNewLeader(leadBroker, a_topic, a_partition, a_port);
            //               continue;

        }
        ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topic, partition);

        for (MessageAndOffset messageAndOffset : byteBufferMessageSet) {

            long currentOffset = messageAndOffset.offset();
            if (currentOffset < readOffset) {
                LOG.warn("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                continue;
            }
            return messageAndOffset.message();
        }

        return null;

    }

    public static Message getLastMessage(String topic, int partition, CuratorFramework curatorFramework) throws Exception {

        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);

        PartitionMetadata metadata = findLeader(brokers, topic, partition);
        if (metadata == null) {
            LOG.error("Can't find metadata for Topic and Partition. Exiting");
            return null;
        }
        if (metadata.leader() == null) {
            LOG.error("Can't find Leader for Topic and Partition. Exiting");
            return null;
        }
        String leadBrokerHost = metadata.leader().host();
        int leadBrokerPort = metadata.leader().port();
        String clientName = "Client_" + topic + "_" + partition;

        SimpleConsumer consumer = new SimpleConsumer(leadBrokerHost, leadBrokerPort, DEFAULT_SO_TIMEOUT_MS, DEFAULT_BUFFER_SIZE, clientName);
        long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);

        LOG.debug("Last offset is {}", readOffset);
        if (readOffset == 0) {
            return null;
        }

        // Rewind offset by 1.
        readOffset = Math.max(0, readOffset - 1);

        Message m = internalReadMessageAtOffset(topic, partition, readOffset, clientName, consumer);

        return m;

    }

    private static PartitionMetadata findLeader(List<String> brokers, String topic, int partition) {
        PartitionMetadata returnMetaData = null;

        List<String> topics = Collections.singletonList(topic);

        loop:
        for (String seed : brokers) {
            SimpleConsumer consumer = null;
            try {
                String[] seedParts = StringUtils.split(seed, ":");
                if (seedParts.length < 1) {
                    continue;
                }
                consumer = new SimpleConsumer(seedParts[0], seedParts.length > 1 ? Integer.valueOf(seedParts[1]) : DEFAULT_ZK_PORT, DEFAULT_SO_TIMEOUT_MS, DEFAULT_BUFFER_SIZE, "leaderLookup");
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {

                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.warn("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "]", e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        return returnMetaData;
    }

    public static Producer<String, String> getProducer(CuratorFramework curatorFramework) throws Exception {

        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
        Preconditions.checkState(brokers.size() > 0);

        Properties props = new Properties();
        props.put("metadata.broker.list", Joiner.on(",").join(brokers));
        props.put("serializer.class", StringEncoder.class.getCanonicalName());
        props.put("request.required.acks", "-1");

        LOG.info("Getting a new producer with the following properties: {}", props);

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        return producer;
    }

    public static void send(Producer<String, String> producer, String topic, String key, String value) {
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key, value);
        producer.send(data);
    }

    private static Random R = new Random();

    public static SimpleConsumer getSimpleConsumer(List<String> brokers, String clientId) throws Exception {
        Preconditions.checkState(brokers.size() > 0);
        String broker = brokers.get(R.nextInt(brokers.size()));
        String[] brokerParts = StringUtils.split(broker, ":");
        Preconditions.checkState(brokerParts.length > 0);
        return new SimpleConsumer(brokerParts[0], brokerParts.length > 1 ? Integer.valueOf(brokerParts[1]) : DEFAULT_ZK_PORT, DEFAULT_SO_TIMEOUT_MS, DEFAULT_BUFFER_SIZE, clientId);
    }

    public static SimpleConsumer getSimpleConsumerForTopicAndPartition(CuratorFramework curatorFramework, String topic, int partition, String clientId) throws Exception {
        PartitionMetadata partitionMetadata = findLeader(KafkaUtils.retrieveBrokerListFromZK(curatorFramework), topic, partition);
        return new SimpleConsumer(partitionMetadata.leader().host(), partitionMetadata.leader().port(), DEFAULT_SO_TIMEOUT_MS, DEFAULT_BUFFER_SIZE, clientId);
    }

    public static Iterator<Message> getMessagesFrom(final String topic, final int partition, final long firstOffset, CuratorFramework curatorFramework) throws Exception {
        return getMessagesFromTo(topic, partition, firstOffset, null, "Client_" + topic + "_" + partition + "_" + firstOffset, curatorFramework);
    }

    public static Iterator<Message> getMessagesFrom(final String topic, final int partition, final long firstOffset, String clientId, CuratorFramework curatorFramework) throws Exception {
        return getMessagesFromTo(topic, partition, firstOffset, null, clientId, curatorFramework);
    }

    public static Iterator<Message> getMessagesFromTo(final String topic, final int partition, final long firstOffset, final Long lastOffset, CuratorFramework curatorFramework) throws Exception {
        return getMessagesFromTo(topic, partition, firstOffset, lastOffset, "Client_" + topic + "_" + partition + "_" + firstOffset + "_" + lastOffset, curatorFramework);
    }

    public static Iterator<Message> getMessagesFromTo(final String topic, final int partition, final long firstOffset, final Long lastOffset, final String clientName, final CuratorFramework curatorFramework) throws Exception {

        return new Iterator<Message>() {
            long currentOffset = firstOffset;
            boolean fetched = false;
            boolean more = true;

            SimpleConsumer consumer;
            Iterator<MessageAndOffset> iterator;

            List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
            int reconnect = 0;
            int numErrors = 0;

            @Override
            public boolean hasNext() {
                fetch();
                boolean r = more && (lastOffset == null || currentOffset < lastOffset) && iterator.hasNext();

                if (!r) {
                    if (consumer != null) {
                        consumer.close();
                    }
                }

                return r;
            }

            @Override
            public Message next() {

                try {
                    fetch();
                    MessageAndOffset m = iterator.next();
                    currentOffset = m.offset(); //m.nextOffset();
                    return m.message();
                } finally {

                }
            }

            @Override
            public void remove() {
                throw new IllegalStateException("Unimplemented -- this is a read-only iterator");
            }

            private void fetch() {

                if (consumer == null) {
                    initConsumer();
                }

                if (iterator == null || !iterator.hasNext()) {

                    do {
                        FetchRequest req = new FetchRequestBuilder()
                                .clientId(clientName)
                                .addFetch(topic, partition, (currentOffset == firstOffset) ? currentOffset : currentOffset + 1, DEFAULT_BUFFER_SIZE)
                                .build();

                        FetchResponse fetchResponse = consumer.fetch(req);

                        if (fetchResponse.hasError()) {

                            numErrors++;

                            short code = fetchResponse.errorCode(topic, partition);
                            LOG.error("Error fetching data from the Broker. " + consumer.host() + ":" + consumer.port() + " Reason: " + code);
                            if (numErrors > 5) {
                                throw new RuntimeException("Too many errors in a row. Aborting");
                            }
                            if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                                more = false;
                                return;
                            }

                            consumer.close();
                            initConsumer();

                        }
                        ByteBufferMessageSet byteBufferMessageSet = fetchResponse.messageSet(topic, partition);
                        iterator = byteBufferMessageSet.iterator();
                        numErrors = 0;
                        break;
                    } while (true);
                }
            }

            private void initConsumer() {
                reconnect++;
//                if (reconnect % 37 == 0) {
//                    brokers = KafkaUtils.retrieveBrokerListFromZK(curatorFramework);
//                }

//                PartitionMetadata metadata = findLeader(brokers, topic, partition);
//                if (metadata == null) {
//                    throw new UnknownTopicOrPartitionException("Can't find metadata for Topic " + topic + " and Partition " + partition);
//                }
//                if (metadata.leader() == null) {
//                    throw new LeaderNotAvailableException("Can't find Leader for Topic " + topic + " and Partition " + partition);
//                }
//                String leadBrokerHost = metadata.leader().host();
//                int leadBrokerPort = metadata.leader().port();

                try {
                    consumer = getSimpleConsumerForTopicAndPartition(curatorFramework, topic, partition, clientName);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

    }
}