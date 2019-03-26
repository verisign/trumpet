package com.verisign.vscc.hdfs.trumpet.server.tool;

import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetServer;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import kafka.message.Message;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class PeekInotifyEvents extends AbstractAppLauncher {

    private static final String OPTION_NUMBER_OF_EVENTS = "numevents";
    private static final int DEFAULT_NUMBER_OF_EVENTS = 1;
    private final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PeekInotifyEvents(), args);
        System.exit(res);
    }

    @Override
    protected int internalRun() throws Exception {

        int numberOfEvents = Integer.parseInt((String) getOptions().valueOf(OPTION_NUMBER_OF_EVENTS));

        Iterator<Message> messagesIterator = SimpleConsumerHelper.getNLastMessages(getTopic(),
                TrumpetServer.PARTITION_NUM, numberOfEvents, getCuratorFrameworkKafka());

        while (messagesIterator.hasNext()) {

            Message m = messagesIterator.next();

            ByteBuffer bb = m.payload().slice();

            try (InputStream in = new ByteBufferInputStream(Collections.singletonList(bb))) {
                JsonNode node = mapper.readTree(in);
                String txIdStr = node.get(EventAndTxId.FIELD_TXID).getValueAsText();
                long currentTxId = Long.parseLong(txIdStr);
                String eventType = node.get(EventAndTxId.FIELD_EVENTTYPE).getValueAsText();
                JsonNode pathNode = node.get(EventAndTxId.FIELD_PATH);
                JsonNode srcPathNode = node.get(EventAndTxId.FIELD_SRCPATH);
                JsonNode dstPathNode = node.get(EventAndTxId.FIELD_DSTPATH);
                String path = pathNode == null ? null : pathNode.getValueAsText();
                String srcPath = srcPathNode == null ? null : srcPathNode.getValueAsText();
                String dstPath = dstPathNode == null ? null : dstPathNode.getValueAsText();

                LOG.info("INotify Event " + currentTxId + " is of type " + eventType + " about");
                if (path != null) LOG.info(" path:" + path);
                if (srcPath != null) LOG.info(" srcPath:" + srcPath);
                if (dstPath != null) LOG.info(" dstPath:" + dstPath);
                LOG.info("");

                if (LOG.isDebugEnabled()) {
                    bb.rewind();
                    try (InputStream in2 = new ByteBufferInputStream(Collections.singletonList(bb))) {
                        final Map<String, Object> o = mapper.readValue(in2, Map.class);
                        LOG.debug("Complete INotify Event is: {}", o);
                    }
                }

            } catch (org.codehaus.jackson.JsonParseException e) {
                LOG.error("Malformated message..." + new String(bb.array(), Charset.defaultCharset()));
            }
        }

        return ReturnCode.ALL_GOOD;
    }

    @Override
    protected void initParser() {
        getParser().accepts(OPTION_NUMBER_OF_EVENTS, "Number of events to peak from the topic")
                .withRequiredArg().defaultsTo(String.valueOf(DEFAULT_NUMBER_OF_EVENTS));
    }
}
