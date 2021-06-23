package com.verisign.vscc.hdfs.trumpet.server.tool;

import com.verisign.vscc.hdfs.trumpet.AbstractAppLauncher;
import com.verisign.vscc.hdfs.trumpet.dto.EventAndTxId;
import com.verisign.vscc.hdfs.trumpet.kafka.ConsumerHelper;
import com.verisign.vscc.hdfs.trumpet.server.TrumpetServer;
import com.verisign.vscc.hdfs.trumpet.utils.TrumpetHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

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

        Iterator<ConsumerRecord<String, String>> recordIterator = ConsumerHelper.getNLastMessages(getTopic(),
                TrumpetServer.PARTITION_NUM, numberOfEvents, getCuratorFrameworkKafka());

        while (recordIterator.hasNext()) {

            ConsumerRecord<String, String> record = recordIterator.next();

            final Map<String, Object> map = TrumpetHelper.toMap(record);
            JsonNode jsonNode = mapper.convertValue(map, JsonNode.class);

            String txIdStr = jsonNode.get(EventAndTxId.FIELD_TXID).asText();
            long currentTxId = Long.parseLong(txIdStr);
            String eventType = jsonNode.get(EventAndTxId.FIELD_EVENTTYPE).asText();
            JsonNode pathNode = jsonNode.get(EventAndTxId.FIELD_PATH);
            JsonNode srcPathNode = jsonNode.get(EventAndTxId.FIELD_SRCPATH);
            JsonNode dstPathNode = jsonNode.get(EventAndTxId.FIELD_DSTPATH);
            String path = pathNode == null ? null : pathNode.asText();
            String srcPath = srcPathNode == null ? null : srcPathNode.asText();
            String dstPath = dstPathNode == null ? null : dstPathNode.asText();

            LOG.info("INotify Event " + currentTxId + " is of type " + eventType + " about");
            if (path != null) LOG.info(" path:" + path);
            if (srcPath != null) LOG.info(" srcPath:" + srcPath);
            if (dstPath != null) LOG.info(" dstPath:" + dstPath);
            LOG.info("");

            if (LOG.isDebugEnabled()) {
                LOG.debug("Complete INotify Event is: {}", record.value());

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
