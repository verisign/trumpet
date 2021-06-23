package com.verisign.vscc.hdfs.trumpet.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.verisign.vscc.hdfs.trumpet.kafka.ConsumerHelper;
import com.verisign.vscc.hdfs.trumpet.kafka.KafkaUtils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static com.verisign.vscc.hdfs.trumpet.utils.TrumpetHelper.toMap;

/**
 * Created by vgrivel on 24.04.2018.
 */

public class InfiniteTrumpetEventConsumer implements Iterable<Map<String, Object>>, AutoCloseable {

    private static Logger LOG = LoggerFactory.getLogger(InfiniteTrumpetEventConsumer.class);

    private static final int NUMBER_OF_THREADS = 1;
    public static final int PARTITION_NUMBER = 0;

    private final String topicName;
    private final Long initialTxId;
    private final Long lastTxId;
    private final String groupId;

    CuratorFramework client;
    private final List<EventStreamIterator> iterators = new LinkedList<>();

    private static final ObjectMapper mapper = new ObjectMapper();

    public InfiniteTrumpetEventConsumer(CuratorFramework client, String topicName, Long initialTxId) throws Exception {
        this(client, topicName, initialTxId, null, InfiniteTrumpetEventConsumer.class.getCanonicalName() + "-" + topicName + "-" + UUID.randomUUID());
    }

    public InfiniteTrumpetEventConsumer(CuratorFramework client, String topicName, Long initialTxId, String groupId) throws Exception {
        this(client, topicName, initialTxId, null, groupId);
    }

    public InfiniteTrumpetEventConsumer(CuratorFramework client, String topicName, String groupId) throws Exception {
        this(client, topicName, 0L, null, groupId);
    }

    public InfiniteTrumpetEventConsumer(CuratorFramework client, String topicName, Long initialTxId, Long lastTxId, String groupId) throws Exception {

        this.client = client;
        this.topicName = topicName;
        this.lastTxId = lastTxId;
        this.groupId = groupId;

        this.initialTxId = initialTxId > 0L ? initialTxId : 0L;
    }

    public InfiniteTrumpetEventConsumer(CuratorFramework client, String topicName) throws Exception {
        this(client, topicName, (Long) null);
    }


    @Override
    public Iterator<Map<String, Object>> iterator() {
        EventStreamIterator iterator = new EventStreamIterator();
        iterators.add(iterator);
        return iterator;
    }

    protected String getZkConnect() {
        return client.getZookeeperClient().getCurrentConnectionString();
    }

    protected String getTopicName() {
        return topicName;
    }

    @Override
    public void close() throws IOException {

    }

    public class EventStreamIterator implements Iterator<Map<String, Object>> {

        private KafkaConsumer consumer;
        private Iterator<ConsumerRecord<String, String>> it;
        private TopicPartition partitionId = new TopicPartition(topicName, PARTITION_NUMBER);
        private long currentOffset = initialTxId;
        private int numErrors = 0;

        @Override
        public boolean hasNext() {
            fetch();
            if (lastTxId != null && currentOffset >= lastTxId) {
                return false;
            } else {
                return it.hasNext();
            }
        }

        @Override
        public Map<String, Object> next() {

            try {
                fetch();
                final ConsumerRecord<String, String> record;

                record = it.next();

                currentOffset = record.offset()+1;
                LOG.trace("EventStreamIterator.next.offset = {}", currentOffset);
                final Map<String, Object> o = toMap(record);
                return o;

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void fetch() {

            if (consumer == null) {
                init();
            }

            if (it == null || !it.hasNext()) {

                do {
                    consumer.seek(partitionId, currentOffset );
                    try {
                        ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                        LOG.trace("number of new messages: {}", records.count());
                        it = records.iterator();
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

        private void init() {
            synchronized (this) {
                if (consumer == null) {
                    try {
                        List<String> brokers = KafkaUtils.retrieveBrokerListFromZK(client);
                        Preconditions.checkState(brokers.size() > 0);
                        Properties props = ConsumerHelper.createConsumerConfig(client, groupId);
                        props.put("enable.auto.commit", false);
                        consumer = new KafkaConsumer<>(props);
                        consumer.assign(Arrays.asList(partitionId));
                        if (initialTxId == 0) {
                            consumer.seekToBeginning(Arrays.asList(partitionId));
                        } else {
                            consumer.seek(partitionId, initialTxId);
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @Override
        public void remove() {
            throw new IllegalStateException("Read only iterator!");
        }
    }
}
