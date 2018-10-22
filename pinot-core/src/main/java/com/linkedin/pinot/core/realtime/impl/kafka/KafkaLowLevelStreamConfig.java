package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import java.util.Map;


/**
 * Wrapper around {@link StreamConfig} for use in {@link KafkaPartitionLevelConsumer}
 */
public class KafkaLowLevelStreamConfig {

  private String _kafkaTopicName;
  private String _bootstrapHosts;

  /**
   * Builds a wrapper around {@link StreamConfig} to fetch kafka partition level consumer related configs
   * @param streamConfig
   */
  public KafkaLowLevelStreamConfig(StreamConfig streamConfig) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

    _kafkaTopicName = streamConfig.getTopicName();

    String llcBrokerListKey =
        KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST);
    _bootstrapHosts = streamConfigMap.get(llcBrokerListKey);
    Preconditions.checkNotNull(_bootstrapHosts,
        "Must specify kafka brokers list " + llcBrokerListKey + " in case of low level kafka consumer");
  }

  public String getKafkaTopicName() {
    return _kafkaTopicName;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
  }

  @Override
  public String toString() {
    return "KafkaLowLevelStreamConfig{" + "_kafkaTopicName='" + _kafkaTopicName + '\'' + ", _bootstrapHosts='"
        + _bootstrapHosts + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    KafkaLowLevelStreamConfig that = (KafkaLowLevelStreamConfig) o;

    return EqualityUtils.isEqual(_kafkaTopicName, that._kafkaTopicName) && EqualityUtils.isEqual(_bootstrapHosts,
        that._bootstrapHosts);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_kafkaTopicName);
    result = EqualityUtils.hashCodeOf(result, _bootstrapHosts);
    return result;
  }
}
