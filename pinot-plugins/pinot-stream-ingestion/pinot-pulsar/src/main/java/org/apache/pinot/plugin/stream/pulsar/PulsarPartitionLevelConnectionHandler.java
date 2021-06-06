package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConnectionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConnectionHandler.class);

  public static final String SEPERATOR = "-";
  public static final String TOPIC_PARTITION_NAME_SUFFIX = "partition";
  protected final PulsarConfig _config;
  protected final String _clientId;
  protected final int _partition;
  protected final String _topic;
  protected PulsarClient _pulsarClient = null;
  protected Reader<byte[]> _reader = null;

  public PulsarPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) {
    _config = new PulsarConfig(streamConfig, clientId);
    _clientId = clientId;
    _partition = partition;
    _topic = _config.getPulsarTopicName();
//    Properties consumerProp = new Properties();
//    consumerProp.putAll(streamConfig.getStreamConfigsMap());
//    consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _config.getBootstrapHosts());
//    consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//    consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
//    if (_config.getKafkaIsolationLevel() != null) {
//      consumerProp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, _config.getKafkaIsolationLevel());
//    }
    try {
      _pulsarClient = PulsarClient.builder().serviceUrl(_config.getBootstrapServers()).build();

      _reader =
          _pulsarClient.newReader().topic(getPartitionedTopicName(partition))
              .startMessageId(_config.getInitialMessageId()).create();

      LOGGER.info("Created consumer with id {} for topic {}", _reader,
          _config.getPulsarTopicName());

    } catch (PulsarClientException e) {
      LOGGER.error("Could not create pulsar consumer", e);
    }

  }

  private String getPartitionedTopicName(int partition) {
    return _config.getPulsarTopicName() + SEPERATOR + TOPIC_PARTITION_NAME_SUFFIX + SEPERATOR + partition;
  }

  public void close()
      throws IOException {
    _reader.close();
  }

  @VisibleForTesting
  protected PulsarConfig getPulsarPartitionLevelStreamConfig() {
    return _config;
  }
}
