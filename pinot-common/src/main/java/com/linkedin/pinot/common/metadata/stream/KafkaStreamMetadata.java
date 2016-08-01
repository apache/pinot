/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata.stream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.google.common.base.Splitter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.Kafka.ConsumerType;
import com.linkedin.pinot.common.utils.StringUtil;
import static com.linkedin.pinot.common.utils.EqualityUtils.hashCodeOf;
import static com.linkedin.pinot.common.utils.EqualityUtils.isEqual;
import static com.linkedin.pinot.common.utils.EqualityUtils.isNullOrNotSameClass;
import static com.linkedin.pinot.common.utils.EqualityUtils.isSameReference;


public class KafkaStreamMetadata implements StreamMetadata {

  private final String _kafkaTopicName;
  private final List<ConsumerType> _consumerTypes = new ArrayList<>(2);
  private final String _zkBrokerUrl;
  private final String _bootstrapHosts;
  private final String _decoderClass;
  private final Map<String, String> _decoderProperties = new HashMap<String, String>();
  private final Map<String, String> _kafkaConsumerProperties = new HashMap<String, String>();
  private final Map<String, String> _streamConfigMap = new HashMap<String, String>();

  public KafkaStreamMetadata(Map<String, String> streamConfigMap) {
    _zkBrokerUrl =
        streamConfigMap.get(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
            Helix.DataSource.Realtime.Kafka.HighLevelConsumer.ZK_CONNECTION_STRING));

    final String bootstrapHostConfigKey = Helix.DataSource.STREAM_PREFIX + "." + Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST;
    if (streamConfigMap.containsKey(bootstrapHostConfigKey)) {
      _bootstrapHosts = streamConfigMap.get(bootstrapHostConfigKey);
    } else {
      _bootstrapHosts = null;
    }

    String consumerTypesCsv =streamConfigMap.get(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.CONSUMER_TYPE));
    Iterable<String> parts = Splitter.on(',').trimResults().split(consumerTypesCsv);
    for (String part : parts) {
      _consumerTypes.add(ConsumerType.valueOf(part));
    }
    if (_consumerTypes.isEmpty()) {
      throw new RuntimeException("Empty consumer types: Must have 'highLevel' or 'simple'");
    }
    Collections.sort(_consumerTypes);

    _kafkaTopicName =
        streamConfigMap.get(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
            CommonConstants.Helix.DataSource.Realtime.Kafka.TOPIC_NAME));
    _decoderClass =
        streamConfigMap.get(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
            CommonConstants.Helix.DataSource.Realtime.Kafka.DECODER_CLASS));
    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(CommonConstants.Helix.DataSource.STREAM_PREFIX + ".")) {
        _streamConfigMap.put(key, streamConfigMap.get(key));
      }
      if (key.startsWith(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
          CommonConstants.Helix.DataSource.Realtime.Kafka.DECODER_PROPS_PREFIX))) {
        _decoderProperties.put(CommonConstants.Helix.DataSource.Realtime.Kafka.getDecoderPropertyKey(key),
            streamConfigMap.get(key));
      }
      if (key.startsWith(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
          Helix.DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX))) {
        _kafkaConsumerProperties.put(CommonConstants.Helix.DataSource.Realtime.Kafka.getConsumerPropertyKey(key),
            streamConfigMap.get(key));
      }
    }
  }

  public boolean hasHighLevelKafkaConsumerType() {
    return _consumerTypes.contains(ConsumerType.highLevel);
  }

  public boolean hasSimpleKafkaConsumerType() {
    return _consumerTypes.contains(ConsumerType.simple);
  }

  public String getKafkaTopicName() {
    return _kafkaTopicName;
  }

  public List<ConsumerType> getConsumerTypes() {
    return _consumerTypes;
  }

  public Map<String, String> getKafkaConfigs() {
    return _streamConfigMap;
  }

  public String getZkBrokerUrl() {
    return _zkBrokerUrl;
  }

  public String getDecoderClass() {
    return _decoderClass;
  }

  public Map<String, String> getDecoderProperties() {
    return _decoderProperties;
  }

  public Map<String, String> getKafkaConsumerProperties() {
    return _kafkaConsumerProperties;
  }

  @Override
  public String toString() {
    final StringBuilder result = new StringBuilder();
    String newline = "\n";
    result.append(this.getClass().getName());
    result.append(" Object {");
    result.append(newline);
    String[] keys = _streamConfigMap.keySet().toArray(new String[0]);
    Arrays.sort(keys);
    for (final String key : keys) {
      if (key.startsWith(StringUtil.join(".", CommonConstants.Helix.DataSource.STREAM_PREFIX,
          CommonConstants.Helix.DataSource.KAFKA))) {
        result.append("  ");
        result.append(key);
        result.append(": ");
        result.append(_streamConfigMap.get(key));
        result.append(newline);
      }
    }
    result.append("}");

    return result.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (isSameReference(this, o)) {
      return true;
    }

    if (isNullOrNotSameClass(this, o)) {
      return false;
    }

    KafkaStreamMetadata that = (KafkaStreamMetadata) o;

    return isEqual(_kafkaTopicName, that._kafkaTopicName) &&
        isEqual(_consumerTypes, that._consumerTypes) &&
        isEqual(_zkBrokerUrl, that._zkBrokerUrl) &&
        isEqual(_decoderClass, that._decoderClass) &&
        isEqual(_decoderProperties, that._decoderProperties) &&
        isEqual(_streamConfigMap, that._streamConfigMap);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(_kafkaTopicName);
    result = hashCodeOf(result, _consumerTypes);
    result = hashCodeOf(result, _zkBrokerUrl);
    result = hashCodeOf(result, _decoderClass);
    result = hashCodeOf(result, _decoderProperties);
    result = hashCodeOf(result, _streamConfigMap);
    return result;
  }

  @Override
  public Map<String, String> toMap() {
    return _streamConfigMap;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
  }
}
