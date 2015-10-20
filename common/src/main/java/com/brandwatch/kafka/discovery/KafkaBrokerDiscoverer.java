package com.brandwatch.kafka.discovery;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Credits to https://github.com/jstanier/kafka-broker-discovery/
 */
public class KafkaBrokerDiscoverer implements Closeable {

    private ConnectionStringCreator connectionStringCreator;
    private BrokerInfoFetcher dataFetcher;
    private List<BrokerInfo> brokers;

    public KafkaBrokerDiscoverer(CuratorFramework curatorFramework) throws Exception {
        Preconditions.checkNotNull(curatorFramework);
        connectionStringCreator = new ConnectionStringCreator();
        dataFetcher = new BrokerInfoFetcher(curatorFramework);
    }

    public String getConnectionString() throws Exception {
        return connectionStringCreator.createConnectionString(dataFetcher.fetchBrokerInfo());
    }

    public void close() throws IOException {
        dataFetcher.close();
    }

}
