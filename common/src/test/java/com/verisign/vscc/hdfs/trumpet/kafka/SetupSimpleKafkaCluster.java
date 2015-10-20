package com.verisign.vscc.hdfs.trumpet.kafka;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by bperroud on 31-Mar-15.
 */
public abstract class SetupSimpleKafkaCluster {

    private int brokerId = 0;
    protected int zkConnectionTimeout = 6000;
    protected int zkSessionTimeout = 6000;

    protected String zkConnect;
    protected EmbeddedZookeeper zkServer;
    protected ZkClient zkClient;
    protected KafkaServer kafkaServer;
    protected String kafkaServersString = "";
    protected List<KafkaServer> servers = new ArrayList<>();

    protected CuratorFramework curatorFramework;

    @Before
    public void setup() throws Exception {

        // setup Zookeeper
        zkConnect = TestZKUtils.zookeeperConnect();
        zkServer = new EmbeddedZookeeper(zkConnect);
        zkClient = new ZkClient(zkServer.connectString(), zkConnectionTimeout, zkSessionTimeout, ZKStringSerializer$.MODULE$);

        // setup Broker
        int port = TestUtils.choosePort();
        Properties props = TestUtils.createBrokerConfig(brokerId, port, true);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
        kafkaServersString += kafkaServer.socketServer().host() + ":" + kafkaServer.socketServer().port();

        curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectionTimeoutMs(zkConnectionTimeout).sessionTimeoutMs(zkSessionTimeout)
                .build();
        curatorFramework.start();
    }

    @After
    public void tearDown() {

        if (curatorFramework != null) {
            curatorFramework.close();
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (zkClient != null) {
            zkClient.close();
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }

    }

    protected static String kafkaServersToListOfString(List<KafkaServer> servers) {
        return Joiner.on(",").join(Iterables.transform(servers, new Function<KafkaServer, String>() {
            @Nullable
            @Override
            public String apply(KafkaServer input) {
                return input.socketServer().host() + ":" + input.socketServer().port();
            }
        }));
    }

}
