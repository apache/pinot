package org.apache.pinot.tools.utils;

import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamDataProvider;
import org.apache.pinot.spi.stream.StreamDataServerStartable;
import org.apache.pinot.spi.utils.NetUtils;


public class KinesisStarterUtils {
  private KinesisStarterUtils(){}

  public static final String DEFAULT_KINESIS_PORT = "4566";
  public static final String DEFAULT_KINESIS_ENDPOINT = "http://localhost:"+DEFAULT_KINESIS_PORT;

  public static final String KINESIS_SERVER_STARTABLE_CLASS_NAME =
      getKinesisConnectorPackageName() + ".server.KinesisDataServerStartable";
  public static final String KINESIS_PRODUCER_CLASS_NAME = getKinesisConnectorPackageName() + ".server.KinesisDataProducer";
  public static final String KINESIS_STREAM_CONSUMER_FACTORY_CLASS_NAME =
      getKinesisConnectorPackageName() + ".KinesisConsumerFactory";

  public static final String PORT = "port";
  public static final String NUM_SHARDS = "numShards";


  private static String getKinesisConnectorPackageName() {
    return "org.apache.pinot.plugin.stream.kinesis";
  }

  public static Properties getTopicCreationProps(int numKafkaPartitions) {
    Properties topicProps = new Properties();
    topicProps.put(NUM_SHARDS, numKafkaPartitions);
    return topicProps;
  }

  public static StreamDataServerStartable startServer(final int port, final Properties baseConf) {
    StreamDataServerStartable kinesisStarter;
    Properties configuration = new Properties(baseConf);
    int kinesisPort = port;
    try {
      configuration.put(KinesisStarterUtils.PORT, kinesisPort);
      kinesisStarter = StreamDataProvider.getServerDataStartable(KINESIS_SERVER_STARTABLE_CLASS_NAME, configuration);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start " + KINESIS_SERVER_STARTABLE_CLASS_NAME, e);
    }
    kinesisStarter.start();
    return kinesisStarter;
  }
}
