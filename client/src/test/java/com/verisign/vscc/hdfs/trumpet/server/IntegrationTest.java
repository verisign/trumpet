package com.verisign.vscc.hdfs.trumpet.server;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.verisign.vscc.hdfs.trumpet.kafka.KafkaUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.qjournal.MiniQJMHACluster;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public abstract class IntegrationTest {

    protected static final long DEFAULT_TIMEOUT = 600000;
    protected static final int BLOCK_SIZE = 1024;

    private int brokerId = 0;
    protected int zkConnectionTimeout = (int)DEFAULT_TIMEOUT;
    protected int zkSessionTimeout = (int)DEFAULT_TIMEOUT;
    private static final String ZKHOST = "127.0.0.1";

    protected final Random r = new Random();

    protected String zkConnect;
    protected TestingCluster zkTestingCluster;
    protected ZkClient zkClient;
    protected KafkaServer kafkaServer;
    protected List<KafkaServer> servers = new ArrayList<>();

    protected MiniQJMHACluster cluster;

    protected CuratorFramework curatorFramework;

    protected TrumpetServerCLI trumpetServerCLI;
    protected String trumpetTopicName;

    private final Executor executor = Executors.newCachedThreadPool();

    protected int totalNumberOfFiles = 0;

    @Before
    public void setup() throws Exception {

        Configuration conf = new HdfsConfiguration();
        conf.setLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, BLOCK_SIZE);
        conf.setBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY, true);

        conf.setBoolean(DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME, true);
        conf.setBoolean(DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME, true);
        conf.set(DFSConfigKeys.DFS_NAMENODE_RPC_BIND_HOST_KEY, "0.0.0.0");
        conf.set(DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY, "0.0.0.0");
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_BIND_HOST_KEY, "0.0.0.0");
        conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_BIND_HOST_KEY, "0.0.0.0");

        // so that we can get an atime change
        conf.setLong(DFSConfigKeys.DFS_NAMENODE_ACCESSTIME_PRECISION_KEY, 1);

        MiniQJMHACluster.Builder builder = new MiniQJMHACluster.Builder(conf);
        builder.getDfsBuilder().numDataNodes(1);
        cluster = builder.build();

        cluster.getDfsCluster().waitActive();
        cluster.getDfsCluster().transitionToActive(0);

        final FileSystem fs = cluster.getDfsCluster().getFileSystem(0);
        int numberOfFiles = r.nextInt(2) + 2;

        for (int i = 0; i < numberOfFiles; i++) {
            DFSTestUtil.createFile(fs, new Path("/file1-" + i), BLOCK_SIZE, (short) 1, 0L);
            totalNumberOfFiles++;
        }

        long l = cluster.getDfsCluster().getFileSystem(0).getClient().getNamenode().rollEdits();

        Assert.assertTrue(l > 0);

        trumpetTopicName = "hdfs_inotify_" + r.nextInt(Integer.MAX_VALUE);

        // setup Zookeeper
        EmbeddedZookeeper zkServer = new EmbeddedZookeeper();
        String zkConnect = ZKHOST + ":" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

        // setup Broker
        int port = TestUtils.RandomPort();

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("port", String.valueOf(port));
        KafkaConfig config = new KafkaConfig(brokerProps);

        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
        servers.add(kafkaServer);

        curatorFramework = CuratorFrameworkFactory.builder().connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .connectionTimeoutMs(zkConnectionTimeout).sessionTimeoutMs(zkSessionTimeout)
                .build();
        curatorFramework.start();
        assertTrue("Failed to connect to Zookeeper " + zkConnect, curatorFramework.blockUntilConnected(60, TimeUnit.SECONDS));

        if (!KafkaUtils.topicExists(trumpetTopicName, curatorFramework)) {
            AdminUtils.createTopic(zkUtils, trumpetTopicName, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
            TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), trumpetTopicName, 0, TimeUnit.SECONDS.toMillis(60));
        }

        byte[] ba = curatorFramework.getData().forPath("/brokers/ids/0");

        System.out.println("zkConnect=" + zkConnect);
        System.out.println("/broker/ids/0=" + new String(ba));
        System.out.println("kafkaConnect=" + kafkaServersToListOfString(servers));

        final TrumpetServerCLI trumpetServerCLI = new TrumpetServerCLI();
        this.trumpetServerCLI = trumpetServerCLI;

        final List<String> argsList = new ArrayList<>();

        argsList.add("--" + TrumpetServerCLI.OPTION_TOPIC);
        argsList.add(trumpetTopicName);

        argsList.add("--" + TrumpetServerCLI.OPTION_ZK_CONNECT);
        argsList.add(zkConnect);

        argsList.add("--" + TrumpetServerCLI.OPTION_DFS_EDITS_DIR);
        argsList.add(cluster.getJournalCluster().getJournalNode(0).getConf().get(DFSConfigKeys.DFS_JOURNALNODE_EDITS_DIR_KEY));

        argsList.add("--" + TrumpetServerCLI.OPTION_BASE_THROTTLE_TIME_MS);
        argsList.add("100");

        final Configuration hdfsConf = cluster.getDfsCluster().getConfiguration(0);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    int res = ToolRunner.run(hdfsConf, trumpetServerCLI, argsList.toArray(new String[0]));
                } catch (Exception e) {
                    System.err.println("Exception occurred in Trumpet");
                    e.printStackTrace();
                }
            }
        });

        do {
            Thread.sleep(1000);
            System.out.println("Waiting for Trumpet to initialize");
        } while (!trumpetServerCLI.isInitialized());

    }


    @After
    public void tearDown() throws IOException {

        if (trumpetServerCLI != null) {
            trumpetServerCLI.close();
        }

        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }

        if (cluster != null) {
            cluster.shutdown();
        }

        if (curatorFramework != null) {
            curatorFramework.close();
        }

        if (zkClient != null) {
            zkClient.close();
        }

        if (zkTestingCluster != null) {
            zkTestingCluster.close();
        }
    }

    protected static String kafkaServersToListOfString(List<KafkaServer> servers) {
        return Joiner.on(",").join(Iterables.transform(servers, new Function<KafkaServer, String>() {
            @Nullable
            @Override
            public String apply(KafkaServer input) {
                return input.socketServer().config().hostName() + ":" + input.socketServer().config().port();
            }
        }));
    }
}
