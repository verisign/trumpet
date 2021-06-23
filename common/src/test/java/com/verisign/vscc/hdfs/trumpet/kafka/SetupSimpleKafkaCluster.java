package com.verisign.vscc.hdfs.trumpet.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Nullable;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.zk.EmbeddedZookeeper;
import scala.Option$;

/**
 * Created by bperroud on 31-Mar-15.
 */
public abstract class SetupSimpleKafkaCluster {

    private int brokerId = 0;
    protected int zkConnectionTimeout = 6000;
    protected int zkSessionTimeout = 6000;

    protected String zkConnect;
    protected EmbeddedZookeeper zkServer;
    protected ZkConnection zkConnection;
    protected ZkClient zkClient;
    protected KafkaServer kafkaServer;
    protected String kafkaServersString = "";
    protected List<KafkaServer> servers = new ArrayList<>();

    protected CuratorFramework curatorFramework;

    @Before
    public void setup() throws Exception {

        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        zkConnect = "127.0.0.1:" + zkServer.zookeeper().getClientPort();

        zkConnection = new ZkConnection(zkConnect,zkSessionTimeout);
        zkClient = new ZkClient(zkConnection, zkConnectionTimeout);

        // setup Broker
        int port = 19092;
        Properties props = TestUtils.createBrokerConfig(brokerId, zkConnect, false, false,
            port, Option$.MODULE$.empty(), Option$.MODULE$.empty(), Option$.MODULE$.empty(), true, false, -1,
            false, -1, false, -1, Option$.MODULE$.empty(), -1, false, 10, (short) 1);

        KafkaConfig config = new KafkaConfig(props);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);
        kafkaServersString = kafkaServersToListOfString(servers);
        System.out.println("KAFKASERVERSTRING: "+kafkaServersString);
        curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectionTimeoutMs(zkConnectionTimeout).sessionTimeoutMs(zkSessionTimeout)
                .build();
        curatorFramework.start();
        Thread.sleep(5000);
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
        if(zkConnection != null){
            try {
                zkConnection.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        servers = new ArrayList<>();

    }

    protected static String kafkaServersToListOfString(List<KafkaServer> servers) {
        return Joiner.on(",").join(Iterables.transform(servers, new Function<KafkaServer, String>() {
            @Nullable
            @Override
            public String apply(KafkaServer input) {
                return "127.0.0.1:" + input.boundPort(ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT));
            }
        }));
    }

}
