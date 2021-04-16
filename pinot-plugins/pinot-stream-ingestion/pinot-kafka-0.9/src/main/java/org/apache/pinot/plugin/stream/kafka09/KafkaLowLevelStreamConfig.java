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
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.EqualityUtils;


/**
 * Wrapper around {@link StreamConfig} for use in {@link KafkaPartitionLevelConsumer}
 */
public class KafkaLowLevelStreamConfig {

  private final String _kafkaTopicName;
  private final String _bootstrapHosts;
  private final int _kafkaBufferSize;
  private final int _kafkaSocketTimeout;
  private final int _kafkaFetcherSizeBytes;
  private final int _kafkaFetcherMinBytes;

  /**
   * Builds a wrapper around {@link StreamConfig} to fetch kafka partition level consumer related configs
   * @param streamConfig
   */
  public KafkaLowLevelStreamConfig(StreamConfig streamConfig) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();

    _kafkaTopicName = streamConfig.getTopicName();

    String llcBrokerListKey = KafkaStreamConfigProperties
        .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST);
    String llcBufferKey = KafkaStreamConfigProperties
        .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE);
    String llcTimeoutKey = KafkaStreamConfigProperties
        .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT);
    String fetcherSizeKey = KafkaStreamConfigProperties
        .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_FETCHER_SIZE_BYTES);
    String fetcherMinBytesKey = KafkaStreamConfigProperties
        .constructStreamProperty(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_FETCHER_MIN_BYTES);
    _bootstrapHosts = streamConfigMap.get(llcBrokerListKey);
    _kafkaBufferSize = getIntConfigWithDefault(streamConfigMap, llcBufferKey,
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT);
    _kafkaSocketTimeout = getIntConfigWithDefault(streamConfigMap, llcTimeoutKey,
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT);
    _kafkaFetcherSizeBytes = getIntConfigWithDefault(streamConfigMap, fetcherSizeKey, _kafkaBufferSize);
    _kafkaFetcherMinBytes = getIntConfigWithDefault(streamConfigMap, fetcherMinBytesKey,
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_FETCHER_MIN_BYTES_DEFAULT);
    Preconditions.checkNotNull(_bootstrapHosts,
        "Must specify kafka brokers list " + llcBrokerListKey + " in case of low level kafka consumer");
  }

  public String getKafkaTopicName() {
    return _kafkaTopicName;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
  }

  public int getKafkaBufferSize() {
    return _kafkaBufferSize;
  }

  public int getKafkaSocketTimeout() {
    return _kafkaSocketTimeout;
  }

  public int getKafkaFetcherSizeBytes() {
    return _kafkaFetcherSizeBytes;
  }

  public int getKafkaFetcherMinBytes() {
    return _kafkaFetcherMinBytes;
  }

  private int getIntConfigWithDefault(Map<String, String> configMap, String key, int defaultValue) {
    String stringValue = configMap.get(key);
    try {
      if (StringUtils.isNotEmpty(stringValue)) {
        return Integer.parseInt(stringValue);
      }
      return defaultValue;
    } catch (NumberFormatException ex) {
      return defaultValue;
    }
  }

  @Override
  public String toString() {
    return "KafkaLowLevelStreamConfig{" + "_kafkaTopicName='" + _kafkaTopicName + '\'' + ", _bootstrapHosts='"
        + _bootstrapHosts + '\'' + ", _kafkaBufferSize='" + _kafkaBufferSize + '\'' + ", _kafkaSocketTimeout='"
        + _kafkaSocketTimeout + '\'' + ", _kafkaFetcherSizeBytes='" + _kafkaFetcherSizeBytes + '\'' + ", _kafkaFetcherMinBytes='"
        + _kafkaFetcherMinBytes + '\'' + '}';
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

    return EqualityUtils.isEqual(_kafkaTopicName, that._kafkaTopicName) && EqualityUtils
        .isEqual(_bootstrapHosts, that._bootstrapHosts) && EqualityUtils
        .isEqual(_kafkaBufferSize, that._kafkaBufferSize) && EqualityUtils
        .isEqual(_kafkaSocketTimeout, that._kafkaSocketTimeout) && EqualityUtils
        .isEqual(_kafkaFetcherSizeBytes, that._kafkaFetcherSizeBytes) && EqualityUtils
        .isEqual(_kafkaFetcherMinBytes, that._kafkaFetcherMinBytes);
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_kafkaTopicName);
    result = EqualityUtils.hashCodeOf(result, _bootstrapHosts);
    result = EqualityUtils.hashCodeOf(result, _kafkaBufferSize);
    result = EqualityUtils.hashCodeOf(result, _kafkaSocketTimeout);
    result = EqualityUtils.hashCodeOf(result, _kafkaFetcherSizeBytes);
    result = EqualityUtils.hashCodeOf(result, _kafkaFetcherMinBytes);
    return result;
  }
}
