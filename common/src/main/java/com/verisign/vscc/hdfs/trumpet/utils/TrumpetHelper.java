package com.verisign.vscc.hdfs.trumpet.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.verisign.vscc.hdfs.trumpet.kafka.KafkaUtils;
import com.verisign.vscc.hdfs.trumpet.kafka.SimpleConsumerHelper;
import kafka.message.Message;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class TrumpetHelper {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Object> toMap(byte[] payload) throws IOException {
        final Map<String, Object> o = mapper.readValue(payload, Map.class);
        return o;
    }

    public static Map<String, Object> toMap(Message message) throws IOException {
        return toMap(KafkaUtils.toByteArray(message));
    }
}
