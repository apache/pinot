/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.realtime.stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamConfigProperties;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.EqualityUtils.*;


/*
 * TODO (Issue 2583)
 * Rename generic member variables from (and accessor methods) to be kafka-agnostic.
 * It is expected that an incoming partitioned stream has the following properties that can be configured in general:
 * - Topic name
 * - Decoder class
 * - Consumer factory name
 * - Connection timeout: (max time to establish a connection/session to the source)
 * - Fetch timeout (max time to wait to fetch messages once a connection is established)
 * - Decoder-specific properties
 * Add a derived class that is Kafka-specific that includes members like zk string and bootstrap hosts.
 */
public class StreamConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamConfig.class);

  private final String _streamType;
  private final String _topicName;
  private final List<ConsumerType> _consumerTypes = new ArrayList<>(2);
  private final String _offsetCriteria;
  private final String _zkBrokerUrl;
  private final String _bootstrapHosts;
  private final String _decoderClass;
  private String _consumerFactoryName;
  private final long _connectionTimeoutMillis;
  private final int _fetchTimeoutMillis;
  private final Map<String, String> _decoderProperties = new HashMap<String, String>();
  private final Map<String, String> _kafkaConsumerProperties = new HashMap<String, String>();
  private final Map<String, String> _streamConfigMap = new HashMap<String, String>();

  private static final long DEFAULT_CONNECTION_TIMEOUT_MILLIS = 30000L;
  private static final int DEFAULT_FETCH_TIMEOUT_MILLIS = 5000;
  private static final String DEFAULT_CONSUMER_FACTORY_CLASS = SimpleConsumerFactory.class.getName();
  private static final String DEFAULT_OFFSET_CRITERIA = "largest";

  public enum ConsumerType {
    SIMPLE,
    HIGHLEVEL
  }

  public StreamConfig(Map<String, String> streamConfigMap) {
    _streamType = streamConfigMap.get(StreamConfigProperties.STREAM_TYPE);
    Preconditions.checkNotNull(_streamType,
        "Must provide stream type in property " + StreamConfigProperties.STREAM_TYPE);

    final String streamTopicProperty =
        StreamConfigProperties.constructStreamProperty(_streamType, StreamConfigProperties.STREAM_TOPIC_NAME);
    _topicName = streamConfigMap.get(streamTopicProperty);
    Preconditions.checkNotNull(_topicName, "Must provide stream topic name in property " + streamTopicProperty);

    // TODO: Move zkBrokerURL and bootstrapHosts to kafka specific config objects
    _zkBrokerUrl = streamConfigMap.get(KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.HighLevelConsumer.KAFKA_HLC_ZK_CONNECTION_STRING));

    final String bootstrapHostConfigKey = KafkaStreamConfigProperties.constructStreamProperty(
        KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BROKER_LIST);
    if (streamConfigMap.containsKey(bootstrapHostConfigKey)) {
      _bootstrapHosts = streamConfigMap.get(bootstrapHostConfigKey);
    } else {
      _bootstrapHosts = null;
    }

    String consumerTypesCsv = streamConfigMap.get(
        StreamConfigProperties.constructStreamProperty(_streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES));
    Iterable<String> parts = Splitter.on(',').trimResults().split(consumerTypesCsv);
    for (String part : parts) {
      _consumerTypes.add(ConsumerType.valueOf(part.toUpperCase()));
    }
    if (_consumerTypes.isEmpty()) {
      throw new RuntimeException("Empty consumer types: Must have 'highLevel' or 'simple'");
    }
    Collections.sort(_consumerTypes);

    String offsetCriteriaProperty = StreamConfigProperties.constructStreamProperty(_streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA);
    String offsetCriteria = streamConfigMap.get(offsetCriteriaProperty);
    if (offsetCriteria == null) {
      _offsetCriteria = DEFAULT_OFFSET_CRITERIA;
    } else {
      _offsetCriteria = offsetCriteria;
    }

    _decoderClass = streamConfigMap.get(
        StreamConfigProperties.constructStreamProperty(_streamType, StreamConfigProperties.STREAM_DECODER_CLASS));

    String consumerFactoryProperty = StreamConfigProperties.constructStreamProperty(_streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS);
    _consumerFactoryName = streamConfigMap.get(consumerFactoryProperty);
    if (_consumerFactoryName == null) {
       _consumerFactoryName = DEFAULT_CONSUMER_FACTORY_CLASS;
    }
    LOGGER.info("Setting consumer factory to {}", _consumerFactoryName);

    final String connectionTimeoutPropertyKey = StreamConfigProperties.constructStreamProperty(_streamType,
        StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS);
    long connectionTimeoutMillis;
    if (streamConfigMap.containsKey(connectionTimeoutPropertyKey)) {
      try {
        connectionTimeoutMillis = Long.parseLong(streamConfigMap.get(connectionTimeoutPropertyKey));
      } catch (Exception e) {
        LOGGER.warn("Caught exception while parsing the connection timeout, defaulting to {} ms", e,
            DEFAULT_CONNECTION_TIMEOUT_MILLIS);
        connectionTimeoutMillis = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
      }
    } else {
      connectionTimeoutMillis = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
    }
    _connectionTimeoutMillis = connectionTimeoutMillis;

    final String fetchTimeoutPropertyKey =
        StreamConfigProperties.constructStreamProperty(_streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS);
    int fetchTimeoutMillis;
    if (streamConfigMap.containsKey(fetchTimeoutPropertyKey)) {
      try {
        fetchTimeoutMillis = Integer.parseInt(streamConfigMap.get(fetchTimeoutPropertyKey));
      } catch (Exception e) {
        LOGGER.warn("Caught exception while parsing the fetch timeout, defaulting to {} ms", e,
            DEFAULT_FETCH_TIMEOUT_MILLIS);
        fetchTimeoutMillis = DEFAULT_FETCH_TIMEOUT_MILLIS;
      }
    } else {
      fetchTimeoutMillis = DEFAULT_FETCH_TIMEOUT_MILLIS;
    }
    _fetchTimeoutMillis = fetchTimeoutMillis;

    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(StreamConfigProperties.STREAM_PREFIX)) {
        _streamConfigMap.put(key, streamConfigMap.get(key));
      }
      String decoderPropPrefix =
          StreamConfigProperties.constructStreamProperty(_streamType, StreamConfigProperties.DECODER_PROPS_PREFIX);
      if (key.startsWith(decoderPropPrefix)) {
        _decoderProperties.put(StreamConfigProperties.getPropertySuffix(key, decoderPropPrefix),
            streamConfigMap.get(key));
      }

      // TODO: Move kafkaConsumerProp to kafka specific StreamConfig
      String kafkaConsumerPropPrefix =
          KafkaStreamConfigProperties.constructStreamProperty(KafkaStreamConfigProperties.KAFKA_CONSUMER_PROP_PREFIX);
      if (key.startsWith(kafkaConsumerPropPrefix)) {
        _kafkaConsumerProperties.put(StreamConfigProperties.getPropertySuffix(key, kafkaConsumerPropPrefix),
            streamConfigMap.get(key));
      }
    }
  }

  public boolean hasHighLevelConsumerType() {
    return _consumerTypes.contains(ConsumerType.HIGHLEVEL);
  }

  public boolean hasLowLevelConsumerType() {
    return _consumerTypes.contains(ConsumerType.SIMPLE);
  }

  public long getConnectionTimeoutMillis() {
    return _connectionTimeoutMillis;
  }

  public int getFetchTimeoutMillis() {
    return _fetchTimeoutMillis;
  }

  public String getTopicName() {
    return _topicName;
  }

  public List<ConsumerType> getConsumerTypes() {
    return _consumerTypes;
  }

  public String getOffsetCriteria() {
    return _offsetCriteria;
  }

  public Map<String, String> getKafkaConfigs() {
    return _streamConfigMap;
  }

  // TODO This is the only Kafka-specific method, used in HLC.
  // Need to figure out a way to move this to a kafka-specific class
  public String getZkBrokerUrl() {
    return _zkBrokerUrl;
  }

  public String getDecoderClass() {
    return _decoderClass;
  }

  public String getConsumerFactoryName() {
    return _consumerFactoryName;
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
      if (key.startsWith(StringUtil.join(".", StreamConfigProperties.STREAM_PREFIX,
          KafkaStreamConfigProperties.STREAM_TYPE))) {
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

    StreamConfig that = (StreamConfig) o;

    return isEqual(_topicName, that._topicName) &&
        isEqual(_consumerTypes, that._consumerTypes) &&
        isEqual(_zkBrokerUrl, that._zkBrokerUrl) &&
        isEqual(_decoderClass, that._decoderClass) &&
        isEqual(_decoderProperties, that._decoderProperties) &&
        isEqual(_streamConfigMap, that._streamConfigMap) &&
        isEqual(_consumerFactoryName, that._consumerFactoryName);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(_topicName);
    result = hashCodeOf(result, _consumerTypes);
    result = hashCodeOf(result, _zkBrokerUrl);
    result = hashCodeOf(result, _decoderClass);
    result = hashCodeOf(result, _decoderProperties);
    result = hashCodeOf(result, _streamConfigMap);
    result = hashCodeOf(result, _consumerFactoryName);
    return result;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
  }
}
