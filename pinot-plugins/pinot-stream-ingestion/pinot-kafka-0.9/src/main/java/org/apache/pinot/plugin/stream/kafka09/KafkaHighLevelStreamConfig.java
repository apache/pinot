/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.stream.kafka09;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * Wrapper around {@link StreamConfig} for use in the {@link KafkaStreamLevelConsumer}
 */
public class KafkaHighLevelStreamConfig {
  private static final String DEFAULT_ZK_SESSION_TIMEOUT_MS = "30000";
  private static final String DEFAULT_ZK_CONNECTION_TIMEOUT_MS = "10000";
  private static final String DEFAULT_ZK_SYNC_TIME = "2000";
  private static final String DEFAULT_REBALANCE_MAX_RETRIES = "30";
  private static final String DEFAULT_REBALANCE_BACKOFF_MS = "2000";
  private static final String DEFAULT_AUTO_COMMIT_ENABLE = "false";

  private static final Map<String, String> defaultProps;

  static {
    defaultProps = new HashMap<>();
    defaultProps
        .put(KafkaStreamConfigProperties.HighLevelConsumer.ZK_SESSION_TIMEOUT_MS, DEFAULT_ZK_SESSION_TIMEOUT_MS);
    defaultProps
        .put(KafkaStreamConfigProperties.HighLevelConsumer.ZK_CONNECTION_TIMEOUT_MS, DEFAULT_ZK_CONNECTION_TIMEOUT_MS);
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.ZK_SYNC_TIME_MS, DEFAULT_ZK_SYNC_TIME);
    // Rebalance retries will take up to 1 mins to fail.
    defaultProps
        .put(KafkaStreamConfigProperties.HighLevelConsumer.REBALANCE_MAX_RETRIES, DEFAULT_REBALANCE_MAX_RETRIES);
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.REBALANCE_BACKOFF_MS, DEFAULT_REBALANCE_BACKOFF_MS);
    defaultProps.put(KafkaStreamConfigProperties.HighLevelConsumer.AUTO_COMMIT_ENABLE, DEFAULT_AUTO_COMMIT_ENABLE);
  }

  private String _kafkaTopicName;
  private String _groupId;
  private String _zkBrokerUrl;
  private Map<String, String> _kafkaConsumerProperties;

  /**
   * Builds a wrapper around {@link StreamConfig} to fetch kafka stream level consumer specific configs
   * @param streamConfig
   * @param tableName
   * @param groupId
   */
  public KafkaHighLevelStreamConfig(StreamConfig streamConfig, String tableName,
      String groupId) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

    _kafkaTopicName = streamConfig.getTopicName();

    String hlcZkBrokerUrlKey = KafkaStreamConfigProperties
        .constructStreamProperty(KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_ZK_CONNECTION_STRING);
    _zkBrokerUrl = streamConfigMap.get(hlcZkBrokerUrlKey);
    Preconditions.checkNotNull(_zkBrokerUrl,
        "Must specify zk broker connect string " + hlcZkBrokerUrlKey + " in high level kafka consumer");
    _groupId = groupId;

    _kafkaConsumerProperties = new HashMap<>();
    String kafkaConsumerPropertyPrefix =
        KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.KAFKA_CONSUMER_PROP_PREFIX);
    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(kafkaConsumerPropertyPrefix)) {
        _kafkaConsumerProperties
            .put(StreamConfigProperties.getPropertySuffix(key, kafkaConsumerPropertyPrefix), streamConfigMap.get(key));
      }
    }
  }

  public String getKafkaTopicName() {
    return _kafkaTopicName;
  }

  public Map<String, Integer> getTopicMap(int numThreads) {
    Map<String, Integer> topicCountMap = new HashMap<>();
    topicCountMap.put(_kafkaTopicName, numThreads);
    return topicCountMap;
  }

  public String getGroupId() {
    return _groupId;
  }

  public String getZkBrokerUrl() {
    return _zkBrokerUrl;
  }

  public Map<String, String> getKafkaConsumerProperties() {
    return _kafkaConsumerProperties;
  }

  public ConsumerConfig getKafkaConsumerConfig() {
    Properties props = new Properties();
    for (String key : defaultProps.keySet()) {
      props.put(key, defaultProps.get(key));
    }
    for (String key : _kafkaConsumerProperties.keySet()) {
      props.put(key, _kafkaConsumerProperties.get(key));
    }
    props.put("group.id", _groupId);
    props.put("zookeeper.connect", _zkBrokerUrl);
    return new ConsumerConfig(props);
  }

  @Override
  public String toString() {
    return "KafkaHighLevelStreamConfig{" + "_kafkaTopicName='" + _kafkaTopicName + '\'' + ", _groupId='" + _groupId
        + '\'' + ", _zkBrokerUrl='" + _zkBrokerUrl + '\'' + ", _kafkaConsumerProperties=" + _kafkaConsumerProperties
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    KafkaHighLevelStreamConfig that = (KafkaHighLevelStreamConfig) o;

    return EqualityUtils.isEqual(_kafkaTopicName, that._kafkaTopicName) && EqualityUtils
        .isEqual(_groupId, that._groupId) && EqualityUtils.isEqual(_zkBrokerUrl, that._zkBrokerUrl) && EqualityUtils
        .isEqual(_kafkaConsumerProperties, that._kafkaConsumerProperties);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_kafkaTopicName);
    result = EqualityUtils.hashCodeOf(result, _groupId);
    result = EqualityUtils.hashCodeOf(result, _zkBrokerUrl);
    result = EqualityUtils.hashCodeOf(result, _kafkaConsumerProperties);
    return result;
  }
}
