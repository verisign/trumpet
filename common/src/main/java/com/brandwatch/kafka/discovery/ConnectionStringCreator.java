package com.brandwatch.kafka.discovery;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.List;

/**
 * Credits to https://github.com/jstanier/kafka-broker-discovery/
 */
public class ConnectionStringCreator {

    private static final Joiner COMMA_JOINER = Joiner.on(",").skipNulls();
    private static final Joiner COLON_JOINER = Joiner.on(":").skipNulls();

    public String createConnectionString(List<BrokerInfo> brokers) {
        Preconditions.checkNotNull(brokers);
        String result = null;
        for (BrokerInfo brokerInfo : brokers) {
            String oneConnectionString = COLON_JOINER.join(brokerInfo.getHost(),
                    brokerInfo.getPort());
            result = COMMA_JOINER.join(result, oneConnectionString);
        }
        return result;
    }

}
