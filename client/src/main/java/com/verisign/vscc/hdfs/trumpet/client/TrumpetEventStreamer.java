package com.verisign.vscc.hdfs.trumpet.client;

import com.verisign.vscc.hdfs.trumpet.kafka.KafkaUtils;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Facilitator class to read Trumpet events from a topic.
 *
 * Supports unbounded reads, from a given transaction or from the head of the topic
 * as well as bounded reads, from and to given transactions.
 */
public class TrumpetEventStreamer implements Iterable<Map<String, Object>> {

    private static Logger LOG = LoggerFactory.getLogger(TrumpetEventStreamer.class);

    public static final int PARTITION_NUMBER = 0;

    private final CuratorFramework curatorFramework;
    private final String topicName;
    private final Long initialTxId;
    private final Long lastTxId;

    private final List<EventStreamIterator> iterators = new LinkedList<>();

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Reads Trumpet events from the given initial transaction id
     * up to the given last transaction id.
     *
     * @param curatorFramework
     * @param topicName
     * @param initialTxId: starting transaction id. If null, start from the head of the topic
     * @param lastTxId: ending transaction id. If not null, the iteration stops when the transaction id is found.
     */
    public TrumpetEventStreamer(CuratorFramework curatorFramework, String topicName, Long initialTxId, Long lastTxId) {
        this.curatorFramework = curatorFramework;
        this.topicName = topicName;
        this.initialTxId = initialTxId;
        this.lastTxId = lastTxId;
    }

    /**
     * Facilitator constructor to infinitely read from Trumpet events from a given transaction id.
     *
     * @param curatorFramework
     * @param topicName
     * @param initialTxId
     */
    public TrumpetEventStreamer(CuratorFramework curatorFramework, String topicName, Long initialTxId) {
        this(curatorFramework, topicName, initialTxId, null);
    }

    /**
     * Facilitator constructor to infinitely read from Trumpet event from the head of the topic.
     *
     * @param curatorFramework
     * @param topicName
     */
    public TrumpetEventStreamer(CuratorFramework curatorFramework, String topicName) {
        this(curatorFramework, topicName, null);
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return new EventStreamIterator();
    }

    protected String getTopicName() {
        return topicName;
    }

    public class EventStreamIterator implements Iterator<Map<String, Object>> {

        Iterator<Message> it;

        @Override
        public boolean hasNext() {
            init();
            return it.hasNext();
        }

        @Override
        public Map<String, Object> next() {

            init();

            try {
                final Message message = it.next();
                final Map<String, Object> o = toMap(message);
                return o;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void init() {
            if (this.it == null) {
                Iterator<Message> it = null;
                try {
                    if (initialTxId != null) {
                        if (lastTxId != null) {
                            it = SimpleConsumerHelper.getMessagesFromTo(getTopicName(), PARTITION_NUMBER, initialTxId, lastTxId, curatorFramework);
                        } else {
                            it = SimpleConsumerHelper.getMessagesFrom(getTopicName(), PARTITION_NUMBER, initialTxId, curatorFramework);
                        }
                    } else {
                        long latestOffset = SimpleConsumerHelper.getLastOffset(curatorFramework, getTopicName(), PARTITION_NUMBER);
                        it = SimpleConsumerHelper.getMessagesFrom(getTopicName(), PARTITION_NUMBER, latestOffset, curatorFramework);
                    }
                } catch (Exception e) {
                    LOG.error("Got an exception", e);
                    throw new RuntimeException(e);
                }
                this.it = it;
            }
        }

        @Override
        public void remove() {
            throw new IllegalStateException("Read only iterator!");
        }
    }

    public static Map<String, Object> toMap(byte[] payload) throws IOException {
        final Map<String, Object> o = mapper.readValue(payload, Map.class);
        return o;
    }

    public static Map<String, Object> toMap(Message message) throws IOException {
        return toMap(KafkaUtils.toByteArray(message));
    }
}
