package com.verisign.vscc.hdfs.trumpet.server.rx;

import com.codahale.metrics.Timer;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.server.exception.EditLogObservableException;
import com.verisign.vscc.hdfs.trumpet.server.exception.ProducerSubscriberException;
import com.verisign.vscc.hdfs.trumpet.server.metrics.Metrics;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Java RX Subscriber, pushing the stream into Kafka.
 * <p/>
 * Note: this class is not responsible from the producer,
 * and it's the caller responsibility to instantiate and close it.
 * <p/>
 * The last published txId is tracked thanks to a AtomicLong.
 * The synchronization feature of the AtomicLong is not used,
 * it is mainly used as a wrapper here.
 */
public class ProducerSubscriber extends Subscriber<Map<String, Object>> {

    private static Logger LOG = LoggerFactory.getLogger(ProducerSubscriber.class);

    private final String topic;
    private final Producer<String, String> producer;
    private final AtomicLong lastSeenTxId;

    private final ObjectMapper mapper = new ObjectMapper();

    public ProducerSubscriber(final String topic, final Producer<String, String> producer, final AtomicLong lastSeenTxId) {
        this.topic = topic;
        this.producer = producer;
        this.lastSeenTxId = lastSeenTxId;
    }

    @Override
    public void onCompleted() {
        // It's not this class' responsibility to close the Producer
    }

    @Override
    public void onError(Throwable throwable) {
        if (throwable instanceof IOException && "Read extra bytes after the terminator!".equals(throwable.getMessage())) {
            // do nothing, i.e. let the main program to continue from lastSeenTxId
            return;
        }
        throw new EditLogObservableException("Exception received from Observable, lastSeenTxId is " + lastSeenTxId.get(), throwable);
    }

    @Override
    public void onNext(Map<String, Object> o) {

        try {
            if (o.containsKey(EventAndTxId.FIELD_EVENTTYPE)) {
                String s = mapper.writeValueAsString(o);
                LOG.debug("message: {}", s);

                ProducerRecord<String, String> message = new ProducerRecord<String, String>(topic, s);

                try (Timer.Context context = Metrics.kafkaSender().time()) {
                    producer.send(message);
                } finally {
                    Metrics.kafkaTransaction().mark();
                }
            }

            lastSeenTxId.set(((Number) o.get(EventAndTxId.FIELD_TXID)).longValue());

        } catch (IOException e) {
            throw new ProducerSubscriberException("Got an exception while executing onNext, lastSeenTxId is " + lastSeenTxId.get(), e);
        }
    }
}
