package com.verisign.vscc.hdfs.trumpet;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.verisign.vscc.hdfs.trumpet.kafka.ConsumerHelper;
import com.verisign.vscc.hdfs.trumpet.kafka.KafkaUtils;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * <pre>
 * public static void main(String[] args) throws Exception {
 *   int res = ToolRunner.run(new Configuration(), new Yourclass extends AbstractAppLauncher, args);
 *   System.exit(res);
 * }
 * </pre>
 */
public abstract class AbstractAppLauncher implements Tool, Closeable {

    public static final String OPTION_ZK_CONNECT = "zk.connect";
    public static final String OPTION_ZK_CONNECT_USER = "zk.connect.user";
    public static final String OPTION_ZK_CONNECT_KAFKA = "zk.connect.kafka";
    public static final String OPTION_TOPIC = "topic";
    public static final String OPTION_PROPERTIES_PRODUCER = "producer.properties.file";
    public static final String OPTION_PROPERTIES_CONSUMER = "consumer.properties.file";
    protected static final String DEFAULT_TOPIC_NAME = "hdfs.inotify.events";
    protected static final String OPTION_HELP = "help";

    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    private final OptionParser parser = new OptionParser();

    private Configuration conf;
    private CuratorFramework curatorFrameworkKafka;
    private CuratorFramework curatorFrameworkUser;
    private OptionSet options;
    private String topic;
    private String zkConnectKafka;
    private String zkConnectUser;

    protected final String getTopic() {
        return topic;
    }

    protected final OptionSet getOptions() {
        return options;
    }

    protected final OptionParser getParser() {
        return parser;
    }

    protected String getZkConnectKafka() {
        return zkConnectKafka;
    }

    protected String getZkConnectUser() {
        return zkConnectUser;
    }

    protected final CuratorFramework getCuratorFrameworkKafka() {
        return curatorFrameworkKafka;
    }

    protected final CuratorFramework getCuratorFrameworkUser() {
        return curatorFrameworkUser;
    }

    @Override
    public final int run(String[] args) throws Exception {

        privateInitParser();

        boolean invalidOptions = false;
        try {
            options = getParser().parse(args);
        } catch (OptionException e) {
            invalidOptions = true;
            System.err.println("Invalid argument: " + e.getMessage());
            System.err.println("Run with --" + OPTION_HELP + " for help.");
        }

        if (invalidOptions || options.has(OPTION_HELP)) {
            getParser().printHelpOn(System.out);
            return ReturnCode.HELP;
        }

        topic = (String) options.valueOf(OPTION_TOPIC);
        String tmpZkConnect = (String) options.valueOf(OPTION_ZK_CONNECT);
        String tmpZkConnectKafka = (String) options.valueOf(OPTION_ZK_CONNECT_KAFKA);
        String tmpZkConnectUser = (String) options.valueOf(OPTION_ZK_CONNECT_USER);
        if ((tmpZkConnect != null && (tmpZkConnectKafka != null || tmpZkConnectUser != null)) ||
                (tmpZkConnect == null && (tmpZkConnectKafka == null || tmpZkConnectUser == null))) {
            System.err.println("Either specify --" + OPTION_ZK_CONNECT + ", or both --" + OPTION_ZK_CONNECT_KAFKA + " and --" + OPTION_ZK_CONNECT_USER + ", but not a mix.");
            return ReturnCode.WRONG_ZK_CONFIG;
        }
        ConsumerHelper.setPathConsumerProperties((String) options.valueOf(OPTION_PROPERTIES_CONSUMER));
        ConsumerHelper.setPathProducerProperties((String) options.valueOf(OPTION_PROPERTIES_PRODUCER));

        if (tmpZkConnect != null) {
            zkConnectKafka = tmpZkConnect;
            zkConnectUser = tmpZkConnect;
            CuratorFramework tmpCuratorFramework = CuratorFrameworkFactory.builder()
                    .connectString(tmpZkConnect)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            tmpCuratorFramework.start();
            curatorFrameworkKafka = tmpCuratorFramework;
            curatorFrameworkUser = tmpCuratorFramework;
        } else {

            zkConnectKafka = tmpZkConnectKafka;
            zkConnectUser = tmpZkConnectUser;
            curatorFrameworkKafka = CuratorFrameworkFactory.builder()
                    .connectString(tmpZkConnectKafka)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            curatorFrameworkKafka.start();

            curatorFrameworkUser = CuratorFrameworkFactory.builder()
                    .connectString(tmpZkConnectKafka)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .build();
            curatorFrameworkUser.start();
        }

        boolean zkConnectionOkKafka = getCuratorFrameworkKafka().blockUntilConnected(5, TimeUnit.SECONDS);
        if (!zkConnectionOkKafka) {
            System.err.println("Can't connect to zookeeper. Please check the --" + OPTION_ZK_CONNECT + " or --" + OPTION_ZK_CONNECT_KAFKA
                    + " arguments (current is " + zkConnectKafka + ")");
            return ReturnCode.CANNOT_CONNECT_TO_ZK;
        }

        boolean zkConnectionOkUser = getCuratorFrameworkKafka().blockUntilConnected(5, TimeUnit.SECONDS);
        if (!zkConnectionOkUser) {
            System.err.println("Can't connect to zookeeper. Please check the --" + OPTION_ZK_CONNECT + " or --" + OPTION_ZK_CONNECT_USER
                    + " arguments (current is " + zkConnectUser + ")");
            return ReturnCode.CANNOT_CONNECT_TO_ZK;
        }


        if (!KafkaUtils.topicExists(topic,curatorFrameworkKafka )) {
            System.err.println("Kafka topic " + topic + " seems not to exist.");
            System.err.println("We do not assume topic auto-creation, so please create it beforehand. ");
            int replica = Math.min(KafkaUtils.retrieveBrokerListFromZK(getCuratorFrameworkKafka()).size(), KafkaUtils.DEFAULT_REPLICATION);
            System.err.println("(from a server where kafka is installed) $ kafka-topics --create --zookeeper " + zkConnectKafka
                    + " --replication-factor " + replica + " --partitions " + KafkaUtils.DEFAULT_NUM_OF_PARTITION + " --topic " + topic);
            return ReturnCode.KAFKA_TOPIC_DOES_NOT_EXIST;
        }

        return internalRun();

    }

    protected abstract int internalRun() throws Exception;

    private void privateInitParser() {
        getParser().accepts(OPTION_ZK_CONNECT, "List of ZK host:port hosts, comma-separated. Either this one is required, " +
                "or both --" + OPTION_ZK_CONNECT_KAFKA + " and --" + OPTION_ZK_CONNECT_USER + " are required.")
                .withRequiredArg();
        getParser().accepts(OPTION_ZK_CONNECT_KAFKA, "If you have two ZK clusters, one for Kafka and one for the user-space application, " +
                "set the Kafka ZK cluster here. Same format than --" + OPTION_ZK_CONNECT)
                .withRequiredArg();
        getParser().accepts(OPTION_ZK_CONNECT_USER, "If you have two ZK clusters, set the user-space ZK cluster here. " +
                "Same format than --" + OPTION_ZK_CONNECT)
                .withRequiredArg();
        getParser().accepts(OPTION_TOPIC, "Name of the kafka topic to publish the inotify events to")
                .withRequiredArg().defaultsTo(DEFAULT_TOPIC_NAME);
        getParser().accepts(OPTION_PROPERTIES_PRODUCER, "Producer properties files with or without security for Kafka")
                .withOptionalArg();
        getParser().accepts(OPTION_PROPERTIES_CONSUMER, "Consumer properties files with or without security for Kafka")
                .withOptionalArg();

        initParser();

        getParser().accepts(OPTION_HELP, "Print this help").isForHelp();
    }

    /**
     * Override this function to add more options to the command line parser.
     */
    protected void initParser() {
    }


    @Override
    public final Configuration getConf() {
        return conf;
    }

    @Override
    public final void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public final void close() throws IOException {
        internalClose();
        getCuratorFrameworkKafka().close();
        getCuratorFrameworkUser().close();
    }

    /**
     * Override this function to close additional resources prior to closing the curator framework
     *
     * @throws IOException
     */
    protected void internalClose() throws IOException {
    }


    protected static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }

    protected class ReturnCode {

        public static final int ALL_GOOD = 0;
        public static final int HELP = 1;
        public static final int CANNOT_CONNECT_TO_ZK = 2;
        public static final int WRONG_ZK_CONFIG = 10;
        public static final int KAFKA_TOPIC_DOES_NOT_EXIST = 3;
        public static final int GENERIC_WRONG_CONFIG = 3;
        public static final int GENERIC_ERROR = 1;
    }
}
