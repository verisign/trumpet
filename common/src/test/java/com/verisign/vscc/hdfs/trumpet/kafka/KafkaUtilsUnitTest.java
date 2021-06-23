package com.verisign.vscc.hdfs.trumpet.kafka;

import com.verisign.vscc.hdfs.trumpet.utils.TrumpetHelper;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.eq;

/**
 * Created by bperroud on 31-Mar-15.
 */
public class KafkaUtilsUnitTest {

    @Test
    public void testEndpointRetrieval() throws IOException {
        String payload = "{\"jmx_port\":9999,\"timestamp\":\"1498042640173\",\"endpoints\":[\"SASL_PLAINTEXT://kafka01:9092\",\"SASL_SSL://kafka01:9093\",\"PLAINTEXT://kafka01:9091\"],\"host\":\"kafka01\",\"version\":3,\"port\":9091}";
        Map<String, Object> stringObjectMap = TrumpetHelper.toMap(payload.getBytes());

        List<String> endpoints = (List<String>)stringObjectMap.get("endpoints");

        String endpointPLAINTEXT = KafkaUtils.getEndpoint(endpoints, "PLAINTEXT");
        assertNotNull(endpointPLAINTEXT);
        assertEquals("kafka01:9091", endpointPLAINTEXT);

        String endpointSASL_PLAINTEXT = KafkaUtils.getEndpoint(endpoints, "SASL_PLAINTEXT");
        assertNotNull(endpointSASL_PLAINTEXT);
        assertEquals("kafka01:9092", endpointSASL_PLAINTEXT);

        String endpointSASL_SSL = KafkaUtils.getEndpoint(endpoints, "SASL_SSL");
        assertNotNull(endpointSASL_SSL);
        assertEquals("kafka01:9093", endpointSASL_SSL);


    }
}
