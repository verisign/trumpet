package com.verisign.vscc.hdfs.trumpet.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import kafka.message.Message;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.verisign.vscc.hdfs.trumpet.utils.TrumpetHelper.toMap;

import java.io.IOException;
import java.util.*;

/**
 * Facilitator class to read Trumpet events from a topic.
 *
 * Supports unbounded reads, from a given transaction or from the head of the topic
 * as well as bounded reads, from and to given transactions.
 */
public class BoundedTrumpetEventStreamer implements Iterable<Map<String, Object>> {

    private static Logger LOG = LoggerFactory.getLogger(BoundedTrumpetEventStreamer.class);

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
    public BoundedTrumpetEventStreamer(CuratorFramework curatorFramework, String topicName, long initialTxId, long lastTxId) {
        this.curatorFramework = curatorFramework;
        this.topicName = topicName;
        this.initialTxId = initialTxId;
        this.lastTxId = lastTxId;
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
                    it = SimpleConsumerHelper.getMessagesFromTo(getTopicName(), PARTITION_NUMBER, initialTxId, lastTxId, curatorFramework);
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

}
