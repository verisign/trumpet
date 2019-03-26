package com.verisign.vscc.hdfs.trumpet.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.IterableLike$class;

import static com.verisign.vscc.hdfs.trumpet.utils.TrumpetHelper.toMap;

import java.io.IOException;
import java.util.*;

/**
 * Created by bperroud on 03.06.15.
 */
public class InfiniteTrumpetEventStreamer implements Iterable<Map<String, Object>>, AutoCloseable {

    private static Logger LOG = LoggerFactory.getLogger(InfiniteTrumpetEventStreamer.class);

    private static final int NUMBER_OF_THREADS = 1;
    public static final int PARTITION_NUMBER = 0;

    private final String topicName;
    private final Long initialTxId;
    private final Long lastTxId;
    private final String groupId;

    CuratorFramework client;
    private final List<EventStreamIterator> iterators = new LinkedList<>();

    private static final ObjectMapper mapper = new ObjectMapper();

    public InfiniteTrumpetEventStreamer(CuratorFramework client, String topicName, Long initialTxId) throws Exception {
        this(client, topicName, initialTxId, null, InfiniteTrumpetEventStreamer.class.getCanonicalName() + "-" + topicName + "-" + UUID.randomUUID());
    }

    public InfiniteTrumpetEventStreamer(CuratorFramework client, String topicName, String groupId) throws Exception {
        this(client, topicName, null, null, groupId);
    }

    public InfiniteTrumpetEventStreamer(CuratorFramework client, String topicName, Long initialTxId, Long lastTxId, String groupId) throws Exception {

        this.client = client;
        this.topicName = topicName;
        this.lastTxId = lastTxId;
        this.groupId = groupId;

        if (initialTxId != null) {
            // ZK set /consumers/{yourConsumerGroup}/offsets/{yourFancyTopic}/{partitionId} {newOffset}
            String zkPath = "/consumers/" + groupId + "/offsets/" + topicName + "/" + BoundedTrumpetEventStreamer.PARTITION_NUMBER;
            byte[] data = Long.toString(initialTxId).getBytes();
            if (client.checkExists().forPath(zkPath) == null) {
                client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(zkPath, data);
            } else {
                client.setData().forPath(zkPath, data);
            }
        } else {
            initialTxId = 0L;
        }

        this.initialTxId = initialTxId;
    }

    public InfiniteTrumpetEventStreamer(CuratorFramework client, String topicName) throws Exception {
        this(client, topicName, (Long)null);
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

        private kafka.javaapi.consumer.ConsumerConnector consumer;
        private ConsumerIterator<byte[], byte[]> it;

        private long currentOffset = initialTxId;
        private MessageAndMetadata<byte[], byte[]> headMessage = null;

        @Override
        public boolean hasNext() {
            init();
            if (lastTxId != null && currentOffset >= lastTxId) {
                return false;
            } else {
                return it.hasNext();
            }
        }

        @Override
        public Map<String, Object> next() {

            init();

            try {
                final MessageAndMetadata<byte[], byte[]> message;
                if (headMessage != null) {
                    message = headMessage;
                    headMessage = null;
                } else {
                    message = it.next();
                }
                currentOffset = message.offset();
                LOG.trace("EventStreamIterator.next.offset = {}", currentOffset);
                final Map<String, Object> o = toMap(message.message());
                return o;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void init() {
            if (consumer == null) {
                synchronized (this) {
                    if (consumer == null) {
                        final Properties consumerProperties = new Properties();
                        consumerProperties.put("zookeeper.connect", getZkConnect());
                        consumerProperties.put("group.id", groupId);
                        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProperties);
                        consumer = Consumer.createJavaConsumerConnector(consumerConfig);

                        final Map<String, Integer> topicCountMap = Collections.singletonMap(getTopicName(), (Integer) NUMBER_OF_THREADS);
                        final Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
                        final List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(getTopicName());

                        KafkaStream<byte[], byte[]> kafkaStream = streams.get(PARTITION_NUMBER);
                        it = kafkaStream.iterator();

                        // KafkaStream is a IterableLike, and the head function is a scala function.
                        // This trick is needed to be able to call the function. See
                        // http://lampwww.epfl.ch/~michelou/scala/using-scala-from-java.html for more details.
                        headMessage = (MessageAndMetadata<byte[], byte[]>)IterableLike$class.head(kafkaStream);
                        if (headMessage != null) {
                            currentOffset = headMessage.offset();
                            LOG.trace("IterableLike$class.head(kafkaStream).offset = {}", currentOffset);
                        }
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
