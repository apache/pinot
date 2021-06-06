package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pulsar.client.api.MessageId;

//TODO: Add properties for batch receive - maxNumMessages, maxNumBytes, timeout.
public class PulsarConfig {
  public static final String STREAM_TYPE = "pulsar";
  public static final String PULSAR_PROP_PREFIX = "consumer.prop";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String START_POSITION = "start_position";
  public static final String DEFAULT_BOOTSTRAP_BROKERS = "pulsar://localhost:6650";

  private String _pulsarTopicName;
  private String _subscriberId;
  private String _bootstrapServers;
  private MessageId _initialMessageId = MessageId.latest;
  private Map<String, String> _pulsarConsumerProperties;

  public PulsarConfig(StreamConfig streamConfig, String subscriberId){
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    _pulsarTopicName = streamConfig.getTopicName();
    _bootstrapServers = streamConfigMap.getOrDefault(BOOTSTRAP_SERVERS, DEFAULT_BOOTSTRAP_BROKERS);

    Preconditions.checkNotNull(_bootstrapServers,
        "Must specify bootstrap broker connect string " + BOOTSTRAP_SERVERS + " in high level pulsar consumer");
    _subscriberId = subscriberId;

    String startPositionProperty = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, START_POSITION);
    String startPosition = streamConfigMap.getOrDefault(startPositionProperty, "latest");
    if(startPosition.equals("earliest")){
      _initialMessageId =  MessageId.earliest;
    } else if(startPosition.equals("latest")) {
      _initialMessageId = MessageId.latest;
    } else {
      try {
        _initialMessageId = MessageId.fromByteArray(startPosition.getBytes());
      } catch (IOException e){
        throw new RuntimeException("Invalid start position found: " + startPosition);
      }
    }

    _pulsarConsumerProperties = new HashMap<>();

    String pulsarConsumerPropertyPrefix =
        StreamConfigProperties.constructStreamProperty(STREAM_TYPE, PULSAR_PROP_PREFIX);
    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(pulsarConsumerPropertyPrefix)) {
        _pulsarConsumerProperties
            .put(StreamConfigProperties.getPropertySuffix(key, pulsarConsumerPropertyPrefix), streamConfigMap.get(key));
      }
    }
  }

  public String getPulsarTopicName() {
    return _pulsarTopicName;
  }

  public String getSubscriberId() {
    return _subscriberId;
  }

  public String getBootstrapServers() {
    return _bootstrapServers;
  }

  public Properties getPulsarConsumerProperties() {
    Properties props = new Properties();
    for (String key : _pulsarConsumerProperties.keySet()) {
      props.put(key, _pulsarConsumerProperties.get(key));
    }

    return props;
  }

  public MessageId getInitialMessageId() {
    return _initialMessageId;
  }
}
