package com.verisign.vscc.hdfs.trumpet.kafka;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

/**
 * Created by bperroud on 31-Mar-15.
 */
public abstract class SetupSimpleKafkaCluster {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    private int brokerId = 0;
    protected int zkConnectionTimeout = 6000;
    protected int zkSessionTimeout = 6000;

    protected String zkConnect;
    protected EmbeddedZookeeper zkServer;
    protected ZkClient zkClient;
    protected ZkUtils zkUtils;
    protected KafkaServer kafkaServer;
    protected String kafkaServersString = "";
    protected List<KafkaServer> servers = new ArrayList<>();

    protected CuratorFramework curatorFramework;

    protected String ZKHOST = "127.0.0.1";
    protected String BROKERHOST = "127.0.0.1";
    protected String BROKERPORT = "9092";

    @Before
    public void setup() throws Exception {

        // setup Zookeeper
        zkServer = new EmbeddedZookeeper();
        zkConnect = ZKHOST + ":" + zkServer.port();
        zkClient = new ZkClient(zkConnect, zkConnectionTimeout, zkSessionTimeout, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        int zkPort = TestUtils.RandomPort();

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("port",String.valueOf(zkPort));
        brokerProps.setProperty("broker.id", String.valueOf(brokerId));
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-test").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://" + BROKERHOST +":" + BROKERPORT);
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();

        kafkaServer = TestUtils.createServer(config, mock);

        servers.add(kafkaServer);
        kafkaServersString += kafkaServer.socketServer().config().hostName() + ":" + kafkaServer.socketServer().config().port();

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
                return input.socketServer().config().hostName()+ ":"
                        + input.socketServer().config().port();
            }
        }));
    }
}
