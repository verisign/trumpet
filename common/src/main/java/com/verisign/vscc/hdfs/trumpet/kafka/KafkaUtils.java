package com.verisign.vscc.hdfs.trumpet.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.shaded.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.client.ZKClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import kafka.cluster.Broker;
import kafka.cluster.EndPoint;
import kafka.zk.KafkaZkClient;
import scala.Option$;
import scala.collection.JavaConverters;

/**
 * Kafka utility functions.
 * <p/>
 * The rational is to have only a dependency on Zookeeper which we have anyway for the leader
 * election and then deduce the broker list from ZK.
 */
public class KafkaUtils {
    private static Logger LOG = LoggerFactory.getLogger(KafkaUtils.class);
    public static final int DEFAULT_NUM_OF_PARTITION = 1;
    public static final int DEFAULT_REPLICATION = 3;

    public static int DEFAULT_ZK_CONNECTION_TIMEOUT = 10000;
    public static int DEFAULT_ZK_SESSION_TIMEOUT = 10000;


    public static List<String> retrieveBrokerListFromZK(CuratorFramework curatorFramework) {

        KafkaZkClient zkClient = KafkaZkClient.apply(
                curatorFramework.getZookeeperClient().getCurrentConnectionString(), false,
                DEFAULT_ZK_SESSION_TIMEOUT, DEFAULT_ZK_CONNECTION_TIMEOUT, 10, Time.SYSTEM,
                "trumpet", "trumpet", Option$.MODULE$.apply("zookeeper-client"),
                Option$.MODULE$.apply(new ZKClientConfig()));

        List<Broker> brokerList =
                JavaConverters.seqAsJavaListConverter(zkClient.getAllBrokersInCluster()).asJava();
        final List<String> brokers = new ArrayList<>(brokerList.size());
        for (Broker broker : brokerList) {

            EndPoint s =
                    broker.endPoint(new ListenerName(ConsumerHelper.DEFAULT_SECURITY_PROTOCOL));
            brokers.add(s.host() + ":" + s.port());
        }
        return brokers;
    }

    public static void createTopic(String topic, CuratorFramework curatorFramework) {
        createTopic(topic, DEFAULT_NUM_OF_PARTITION, DEFAULT_REPLICATION, curatorFramework);
    }

    public static void createTopic(String topic, int partitions, int replication,
            CuratorFramework curatorFramework) {
        Preconditions.checkArgument(partitions > 0);
        Preconditions.checkArgument(replication > 0);

        try {
            AdminClient adminClient =
                    AdminClient.create(ConsumerHelper.createConsumerConfig(curatorFramework));

            final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);

            final CreateTopicsResult result = adminClient.createTopics(ImmutableList.of(newTopic));
            result.all().get();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create topic:" + topic, e);
        }

    }

    public static boolean topicExists(String topic, CuratorFramework curatorFramework)
            throws ExecutionException, InterruptedException {
        AdminClient admin;
        try {
            admin = AdminClient.create(ConsumerHelper.createConsumerConfig(curatorFramework));
            ListTopicsResult listTopics = admin.listTopics();
            Set<String> names = listTopics.names().get();
            return names.contains(topic);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return false;

    }


    public static String getEndpoint(List<String> endpoints, String securityProtocol)
            throws IOException {
        for (String endpoint : endpoints) {
            if (endpoint.startsWith(securityProtocol + "://")) {
                return endpoint.substring((securityProtocol + "://").length());
            }
        }
        return null;
    }

    public static Properties loadPropertiesFromFile(String absolutePath) {
        Properties prop = new Properties();
        FileInputStream configStream = null;
        try {
            configStream = new FileInputStream(absolutePath);
            prop.load(configStream);

        } catch (FileNotFoundException e) {
            LOG.warn("Properties files not found. Try via the resources");

            InputStream inputStream =
                    KafkaUtils.class.getClassLoader().getResourceAsStream(absolutePath);
            try {
                prop.load(inputStream);

            } catch (IOException e1) {
                LOG.error("Cannot load properties files from Resources");
            } finally {
                try {
                    inputStream.close();
                } catch (IOException e1) {
                    LOG.error("Cannot close the input stream");
                }
            }

        } catch (IOException e) {
            LOG.error("Cannot load properties files");
        } finally {
            try {
                if (configStream != null)
                    configStream.close();
            } catch (IOException e) {
                LOG.error("Cannot close the config stream");
            }
        }
        return prop;
    }
}
