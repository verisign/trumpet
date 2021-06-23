package com.verisign.vscc.hdfs.trumpet.utils;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;


public class TrumpetHelper {

    private static final ObjectMapper mapper = new ObjectMapper();

    public static Map<String, Object> toMap(byte[] payload) throws IOException {
        final Map<String, Object> o = mapper.readValue(payload, Map.class);
        return o;
    }

    public static Map<String, Object> toMap(String value) throws IOException {
        final Map<String, Object> o = mapper.readValue(value, Map.class);
        return o;
    }

    public static Map<String, Object> toMap(ConsumerRecord<String, String> record) throws IOException {
        return toMap(record.value());
    }
}
