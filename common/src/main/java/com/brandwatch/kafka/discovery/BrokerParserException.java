package com.brandwatch.kafka.discovery;

/**
 * Credits to https://github.com/jstanier/kafka-broker-discovery/
 */
public class BrokerParserException extends Exception {

    public BrokerParserException(String message) {
        super(message);
    }

    private static final long serialVersionUID = 1L;

}
