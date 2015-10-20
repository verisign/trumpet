package com.brandwatch.kafka.discovery;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.zookeeper.KeeperException.NoNodeException;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Credits to https://github.com/jstanier/kafka-broker-discovery/
 */
public class BrokerInfoFetcher implements Closeable {

    private CuratorFramework curatorFramework;
    private PathChildrenCache pathChildrenCache;
    private String hostAndPort;
    private boolean initialised = false;

    public BrokerInfoFetcher(CuratorFramework curatorFramework) throws Exception {
        Preconditions.checkNotNull(curatorFramework);
        this.curatorFramework = curatorFramework;
    }

    public List<BrokerInfo> fetchBrokerInfo() throws BrokerParserException, Exception {
        if (!initialised) {
            initialisePathChildrenCache();
        }

        List<BrokerInfo> brokerInfos = new ArrayList<BrokerInfo>();
        List<ChildData> currentData = pathChildrenCache.getCurrentData();
        for (ChildData childData : currentData) {
            String data = new String(childData.getData());
            DataParser dataParser = makeDataParser();
            BrokerInfo brokerInfo = dataParser.parseData(data);
            brokerInfo.setId(parseBrokerId(childData.getPath()));
            brokerInfos.add(brokerInfo);
        }
        return getTopThree(brokerInfos);
    }

    private int parseBrokerId(String path) {
        return Integer.parseInt(path.substring(path.lastIndexOf("/") + 1, path.length()));
    }

    protected DataParser makeDataParser() {
        return new DataParser();
    }

    private List<BrokerInfo> getTopThree(List<BrokerInfo> brokerInfos) {
        Collections.sort(brokerInfos, new Comparator<BrokerInfo>() {

            public int compare(BrokerInfo left, BrokerInfo right) {
                return new CompareToBuilder().append(left.getId(), right.getId()).toComparison();
            }
        });

        if (brokerInfos.size() < 3) {
            return brokerInfos;
        } else {
            return brokerInfos.subList(0, 3);
        }
    }

    private void initialisePathChildrenCache() throws Exception {
        if (curatorFramework.checkExists().forPath("/brokers/ids") != null) {
            pathChildrenCache = makePathChildrenCache();
            pathChildrenCache.start(StartMode.BUILD_INITIAL_CACHE);
        } else {
            throw new NoNodeException("There is no node at /brokers/ids");
        }
        initialised = true;
    }

    PathChildrenCache makePathChildrenCache() {
        return new PathChildrenCache(curatorFramework, "/brokers/ids", true);
    }

    public void close() throws IOException {
        pathChildrenCache.close();
        //curatorFramework.close();
    }

}
