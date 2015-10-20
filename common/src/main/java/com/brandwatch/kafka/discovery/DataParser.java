package com.brandwatch.kafka.discovery;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Credits to https://github.com/jstanier/kafka-broker-discovery/
 */
public class DataParser {

    private static Splitter COMMA_SPLITTER = Splitter.on(",");
    private static Splitter COLON_SPLITTER = Splitter.on(":");
    private static Set<String> VALID_KEYS = Sets.newHashSet("host", "port", "version", "timestamp",
            "jmx_port");

    public BrokerInfo parseData(String data) throws BrokerParserException {
        Preconditions.checkNotNull(data);
        data = data.trim();

        checkForEmptyString(data);

        BrokerInfo brokerInfo = new BrokerInfo();

        validateBraces(data);

        data = trimFirstAndLastCharacter(data);
        Iterable<String> splitByComma = COMMA_SPLITTER.split(data);
        for (String keyValue : splitByComma) {
            Iterable<String> splitByColon = COLON_SPLITTER.split(keyValue);
            int counter = 0;
            List<String> keyAndValue = new ArrayList<String>();
            for (String keyOrValue : splitByColon) {
                keyAndValue.add(keyOrValue.trim());
                counter++;
            }
            validateKeyValueSize(counter);
            setKeyAndValue(brokerInfo, keyAndValue);
        }
        validateBrokerInfo(brokerInfo);
        return brokerInfo;
    }

    private void validateBraces(String data) throws BrokerParserException {
        if (!data.startsWith("{")) {
            throw new BrokerParserException("Broker configuration should start with {");
        } else if (!data.endsWith("}")) {
            throw new BrokerParserException("Broker configuration should end with }");
        }
    }

    private void setKeyAndValue(BrokerInfo brokerInfo, List<String> keyAndValue)
            throws BrokerParserException {
        String key = getKey(keyAndValue);
        String value = getValue(keyAndValue);
        setValues(brokerInfo, key, value);
    }

    private void validateKeyValueSize(int counter) throws BrokerParserException {
        if (counter != 2) {
            throw new BrokerParserException("Keys and values should be separated by colons");
        }
    }

    private String getValue(List<String> keyAndValue) throws BrokerParserException {
        String value = keyAndValue.get(1);
        value = trimDoubleQuotes(value);
        validateValue(value);
        return value;
    }

    private void validateValue(String value) throws BrokerParserException {
        if (Strings.isNullOrEmpty(value)) {
            throw new BrokerParserException("Null or empty values are not allowed");
        }
    }

    private String getKey(List<String> keyAndValue) throws BrokerParserException {
        String key = keyAndValue.get(0);
        key = trimDoubleQuotes(key);
        validateKey(key);
        return key;
    }

    private void setValues(BrokerInfo brokerInfo, String key, String value)
            throws BrokerParserException {
        if (key.equals("host")) {
            brokerInfo.setHost(value);
        } else if (key.equals("port")) {
            brokerInfo.setPort(Integer.parseInt(value));
        } else if (key.equals("jmx_port")) {
            brokerInfo.setJmxPort(Integer.parseInt(value));
        } else if (key.equals("timestamp")) {
            brokerInfo.setTimestamp(new Date(Long.parseLong(value)));
        } else if (key.equals("version")) {
            brokerInfo.setVersion(Integer.parseInt(value));
        } else {
            throw new BrokerParserException("Unknown key: " + key);
        }
    }

    private void validateBrokerInfo(BrokerInfo brokerInfo) throws BrokerParserException {
        if (Strings.isNullOrEmpty(brokerInfo.getHost())) {
            throw new BrokerParserException("Host value missing");
        } else if (brokerInfo.getPort() < 1) {
            throw new BrokerParserException("Invalid port value");
//        } else if (brokerInfo.getJmxPort() < 1) {
//            throw new BrokerParserException("Invalid JMX port value " + brokerInfo.getJmxPort());
        } else if (brokerInfo.getVersion() < 1) {
            throw new BrokerParserException("Invalid version value");
        } else if (brokerInfo.getTimestamp() == null) {
            throw new BrokerParserException("Invalid timestamp value");
        }
    }

    private void validateKey(String key) throws BrokerParserException {
        if (!VALID_KEYS.contains(key)) {
            throw new BrokerParserException("The key " + key + " is not a valid key.");
        }
    }

    private String trimDoubleQuotes(String word) {
        if (word.startsWith("\"") && word.endsWith("\"")) {
            return trimFirstAndLastCharacter(word);
        } else {
            return word;
        }
    }

    private String trimFirstAndLastCharacter(String data) {
        data = data.substring(1, data.length());
        data = data.substring(0, data.length() - 1);
        return data.trim();
    }

    private void checkForEmptyString(String data) throws BrokerParserException {
        if (data.isEmpty()) {
            throw new BrokerParserException("Broker configuration is empty");
        }
    }
}
