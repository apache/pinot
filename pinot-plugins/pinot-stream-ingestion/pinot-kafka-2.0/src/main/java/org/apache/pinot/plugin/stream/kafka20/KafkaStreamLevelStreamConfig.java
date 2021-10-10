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
package org.apache.pinot.plugin.stream.kafka20;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * Wrapper around {@link StreamConfig} for use in the {@link KafkaStreamLevelConsumer}
 */
public class KafkaStreamLevelStreamConfig {
  private static final String DEFAULT_AUTO_COMMIT_ENABLE = "false";
  private static final Map<String, String> DEFAULT_PROPS = new HashMap<String, String>() {{
    put(KafkaStreamConfigProperties.HighLevelConsumer.AUTO_COMMIT_ENABLE, DEFAULT_AUTO_COMMIT_ENABLE);
  }};

  private String _kafkaTopicName;
  private String _groupId;
  private String _bootstrapServers;
  private Map<String, String> _kafkaConsumerProperties;

  /**
   * Builds a wrapper around {@link StreamConfig} to fetch kafka stream level consumer specific configs
   * @param streamConfig
   * @param tableName
   * @param groupId
   */
  public KafkaStreamLevelStreamConfig(StreamConfig streamConfig, String tableName, String groupId) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

    _kafkaTopicName = streamConfig.getTopicName();
    String hlcBootstrapBrokerUrlKey =
        KafkaStreamConfigProperties
            .constructStreamProperty(KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_BOOTSTRAP_SERVER);
    _bootstrapServers = streamConfigMap.get(hlcBootstrapBrokerUrlKey);
    Preconditions.checkNotNull(_bootstrapServers,
        "Must specify bootstrap broker connect string " + hlcBootstrapBrokerUrlKey + " in high level kafka consumer");
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

  public String getGroupId() {
    return _groupId;
  }

  public Properties getKafkaConsumerProperties() {
    Properties props = new Properties();
    for (String key : DEFAULT_PROPS.keySet()) {
      props.put(key, DEFAULT_PROPS.get(key));
    }
    for (String key : _kafkaConsumerProperties.keySet()) {
      props.put(key, _kafkaConsumerProperties.get(key));
    }
    props.put("group.id", _groupId);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _bootstrapServers);
    return props;
  }

  @Override
  public String toString() {
    return "KafkaStreamLevelStreamConfig{" + "_kafkaTopicName='" + _kafkaTopicName + '\'' + ", _groupId='" + _groupId
        + '\'' + ", _bootstrapServers='"
        + _bootstrapServers + '\'' + ", _kafkaConsumerProperties=" + _kafkaConsumerProperties + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    KafkaStreamLevelStreamConfig that = (KafkaStreamLevelStreamConfig) o;

    return EqualityUtils.isEqual(_kafkaTopicName, that._kafkaTopicName) && EqualityUtils
        .isEqual(_groupId, that._groupId) && EqualityUtils
        .isEqual(_bootstrapServers, that._bootstrapServers) && EqualityUtils
        .isEqual(_kafkaConsumerProperties, that._kafkaConsumerProperties);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_kafkaTopicName);
    result = EqualityUtils.hashCodeOf(result, _groupId);
    result = EqualityUtils.hashCodeOf(result, _bootstrapServers);
    result = EqualityUtils.hashCodeOf(result, _kafkaConsumerProperties);
    return result;
  }

  public String getBootstrapServers() {
    return _bootstrapServers;
  }
}
