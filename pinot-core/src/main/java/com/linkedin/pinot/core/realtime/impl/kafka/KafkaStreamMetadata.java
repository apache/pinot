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
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.base.Splitter;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.Realtime.Kafka.ConsumerType;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.common.utils.retry.RetryPolicies;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumer;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumerFactory;
import com.linkedin.pinot.core.realtime.stream.StreamMetadata;
import com.linkedin.pinot.core.realtime.stream.StreamProviderConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.pinot.common.utils.EqualityUtils.*;


/**
 * Implementation of StreamMetadata which defines metadata for kafka stream
 */
public class KafkaStreamMetadata implements StreamMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamMetadata.class);

  private final String _streamType;
  private final String _kafkaTopicName;
  private final List<ConsumerType> _consumerTypes = new ArrayList<>(2);
  private final String _zkBrokerUrl;
  private final String _bootstrapHosts;
  private final String _decoderClass;
  private String _consumerFactoryName;
  private final long _kafkaConnectionTimeoutMillis;
  private final int _kafkaFetchTimeoutMillis;
  private final Map<String, String> _decoderProperties = new HashMap<String, String>();
  private final Map<String, String> _kafkaConsumerProperties = new HashMap<String, String>();
  private final Map<String, String> _streamConfigMap = new HashMap<String, String>();
  private PinotStreamConsumerFactory _pinotStreamConsumerFactory;

  private static final long DEFAULT_KAFKA_CONNECTION_TIMEOUT_MILLIS = 30000L;
  private static final int DEFAULT_KAFKA_FETCH_TIMEOUT_MILLIS = 5000;

  public KafkaStreamMetadata(Map<String, String> streamConfigMap) {

    _streamType = streamConfigMap.get(CommonConstants.Helix.DataSource.Realtime.STREAM_TYPE);

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
        streamConfigMap.get(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
            Helix.DataSource.Realtime.Kafka.TOPIC_NAME));
    _decoderClass =
        streamConfigMap.get(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
            Helix.DataSource.Realtime.Kafka.DECODER_CLASS));

    _consumerFactoryName = streamConfigMap.get(StringUtil
        .join(".", Helix.DataSource.STREAM_PREFIX, Helix.DataSource.Realtime.Kafka.CONSUMER_FACTORY));
    if (_consumerFactoryName == null) {
      _consumerFactoryName = Helix.DataSource.Realtime.Kafka.ConsumerFactory.SIMPLE_CONSUMER_FACTORY_STRING;
    }
    LOGGER.info("Setting consumer factory to {}", _consumerFactoryName);

    _pinotStreamConsumerFactory = PinotStreamConsumerFactory.create(_consumerFactoryName);

    final String kafkaConnectionTimeoutPropertyKey = StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
        Helix.DataSource.Realtime.Kafka.KAFKA_CONNECTION_TIMEOUT_MILLIS);
    long kafkaConnectionTimeoutMillis;
    if (streamConfigMap.containsKey(kafkaConnectionTimeoutPropertyKey)) {
      try {
        kafkaConnectionTimeoutMillis = Long.parseLong(streamConfigMap.get(kafkaConnectionTimeoutPropertyKey));
      } catch (Exception e) {
        LOGGER.warn("Caught exception while parsing the Kafka connection timeout, defaulting to {} ms", e,
            DEFAULT_KAFKA_CONNECTION_TIMEOUT_MILLIS);
        kafkaConnectionTimeoutMillis = DEFAULT_KAFKA_CONNECTION_TIMEOUT_MILLIS;
      }
    } else {
      kafkaConnectionTimeoutMillis = DEFAULT_KAFKA_CONNECTION_TIMEOUT_MILLIS;
    }
    _kafkaConnectionTimeoutMillis = kafkaConnectionTimeoutMillis;

    final String kafkaFetchTimeoutPropertyKey = StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
        Helix.DataSource.Realtime.Kafka.KAFKA_FETCH_TIMEOUT_MILLIS);
    int kafkaFetchTimeoutMillis;
    if (streamConfigMap.containsKey(kafkaFetchTimeoutPropertyKey)) {
      try {
        kafkaFetchTimeoutMillis = Integer.parseInt(streamConfigMap.get(kafkaFetchTimeoutPropertyKey));
      } catch (Exception e) {
        LOGGER.warn("Caughe exception while parsing the Kafka fetch timeout, defaulting to {} ms", e,
            DEFAULT_KAFKA_FETCH_TIMEOUT_MILLIS);
        kafkaFetchTimeoutMillis = DEFAULT_KAFKA_FETCH_TIMEOUT_MILLIS;
      }
    } else {
      kafkaFetchTimeoutMillis = DEFAULT_KAFKA_FETCH_TIMEOUT_MILLIS;
    }
    _kafkaFetchTimeoutMillis = kafkaFetchTimeoutMillis;

    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(Helix.DataSource.STREAM_PREFIX + ".")) {
        _streamConfigMap.put(key, streamConfigMap.get(key));
      }
      if (key.startsWith(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
          Helix.DataSource.Realtime.Kafka.DECODER_PROPS_PREFIX))) {
        _decoderProperties.put(Helix.DataSource.Realtime.Kafka.getDecoderPropertyKey(key),
            streamConfigMap.get(key));
      }
      if (key.startsWith(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
          Helix.DataSource.Realtime.Kafka.KAFKA_CONSUMER_PROPS_PREFIX))) {
        _kafkaConsumerProperties.put(Helix.DataSource.Realtime.Kafka.getConsumerPropertyKey(key),
            streamConfigMap.get(key));
      }
    }
  }

  @Override
  public String getStreamName() {
    return _kafkaTopicName;
  }

  @Override
  public String getStreamType() {
   return _streamType;
  }

  @Override
  public StreamProviderConfig createStreamProviderConfig() {
    return new KafkaLowLevelStreamProviderConfig();
  }

  @Override
  public boolean hasHighLevelConsumerType() {
    return _consumerTypes.contains(ConsumerType.highLevel);
  }

  @Override
  public boolean hasSimpleConsumerType() {
    return _consumerTypes.contains(ConsumerType.simple);
  }

  @Override
  public List<ConsumerType> getConsumerTypes() {
    return _consumerTypes;
  }

  @Override
  public int getPartitionCount() {
    KafkaPartitionsCountFetcher fetcher = new KafkaPartitionsCountFetcher(this);
    try {
      RetryPolicies.noDelayRetryPolicy(3).attempt(fetcher);
      return fetcher.getPartitionCount();
    } catch (Exception e) {
      Exception fetcherException = fetcher.getException();
      LOGGER.error("Could not get partition count for {}", getStreamName(), fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }

  @Override
  public String getInitialConsumerOffsetCriteria() {
    return getKafkaConsumerProperties().get(CommonConstants.Helix.DataSource.Realtime.Kafka.AUTO_OFFSET_RESET);
  }

  @Override
  public long getPartitionOffset(int partitionId, String offsetCriteria) {

    KafkaOffsetFetcher kafkaOffsetFetcher = new KafkaOffsetFetcher(offsetCriteria, partitionId, this);
    try {
      RetryPolicies.fixedDelayRetryPolicy(3, 1000L).attempt(kafkaOffsetFetcher);
      return kafkaOffsetFetcher.getOffset();
    } catch (Exception e) {
      Exception fetcherException = kafkaOffsetFetcher.getException();
      LOGGER.error("Could not get offset for topic {} partition {}, criteria {}",
          getStreamName(), partitionId, offsetCriteria, fetcherException);
      throw new RuntimeException(fetcherException);
    }
  }

  @Override
  public long getConnectionTimeoutMillis() {
    return _kafkaConnectionTimeoutMillis;
  }

  @Override
  public int getFetchTimeoutMillis() {
    return _kafkaFetchTimeoutMillis;
  }

  @Override
  public PinotStreamConsumerFactory getPinotStreamConsumerFactory() {
    return _pinotStreamConsumerFactory;
  }

  String getZkBrokerUrl() {
    return _zkBrokerUrl;
  }

  String getDecoderClass() {
    return _decoderClass;
  }

  Map<String, String> getDecoderProperties() {
    return _decoderProperties;
  }

  Map<String, String> getKafkaConsumerProperties() {
    return _kafkaConsumerProperties;
  }

  public String getBootstrapHosts() {
    return _bootstrapHosts;
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
      if (key.startsWith(StringUtil.join(".", Helix.DataSource.STREAM_PREFIX,
          Helix.DataSource.KAFKA))) {
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

    KafkaStreamMetadata
        that = (KafkaStreamMetadata) o;

    return isEqual(_kafkaTopicName, that._kafkaTopicName) &&
        isEqual(_consumerTypes, that._consumerTypes) &&
        isEqual(_zkBrokerUrl, that._zkBrokerUrl) &&
        isEqual(_decoderClass, that._decoderClass) &&
        isEqual(_decoderProperties, that._decoderProperties) &&
        isEqual(_streamConfigMap, that._streamConfigMap) &&
        isEqual(_consumerFactoryName, that._consumerFactoryName);
  }

  @Override
  public int hashCode() {
    int result = hashCodeOf(_kafkaTopicName);
    result = hashCodeOf(result, _consumerTypes);
    result = hashCodeOf(result, _zkBrokerUrl);
    result = hashCodeOf(result, _decoderClass);
    result = hashCodeOf(result, _decoderProperties);
    result = hashCodeOf(result, _streamConfigMap);
    result = hashCodeOf(result, _consumerFactoryName);
    return result;
  }


  private static class KafkaPartitionsCountFetcher implements Callable<Boolean> {
    private int _partitionCount = -1;
    private final KafkaStreamMetadata _kafkaStreamMetadata;
    private Exception _exception;

    private KafkaPartitionsCountFetcher(KafkaStreamMetadata kafkaStreamMetadata) {
      _kafkaStreamMetadata = kafkaStreamMetadata;
    }

    private int getPartitionCount() {
      return _partitionCount;
    }

    private Exception getException() {
      return _exception;
    }

    @Override
    public Boolean call() throws Exception {
      final String bootstrapHosts = _kafkaStreamMetadata.getBootstrapHosts();
      final String kafkaTopicName = _kafkaStreamMetadata.getStreamName();
      if (bootstrapHosts == null || bootstrapHosts.isEmpty()) {
        throw new RuntimeException("Invalid value for " + Helix.DataSource.Realtime.Kafka.KAFKA_BROKER_LIST);
      }
      PinotStreamConsumer consumerWrapper = _kafkaStreamMetadata.getPinotStreamConsumerFactory().buildMetadataFetcher(
          KafkaStreamMetadata.class.getSimpleName() + "-" + kafkaTopicName, _kafkaStreamMetadata);
      try {
        _partitionCount = consumerWrapper.getPartitionCount(kafkaTopicName, /*maxWaitTimeMs=*/5000L);
        if (_exception != null) {
          // We had at least one failure, but succeeded now. Log an info
          LOGGER.info("Successfully retrieved partition count as {} for {}", _partitionCount, kafkaTopicName);
        }
        return Boolean.TRUE;
      } catch (SimpleConsumerWrapper.TransientConsumerException e) {
        LOGGER.warn("Could not get Kafka partition count for {}:{}", kafkaTopicName, e.getMessage());
        _exception = e;
        return Boolean.FALSE;
      } catch (Exception e) {
        _exception = e;
        throw e;
      } finally {
        IOUtils.closeQuietly(consumerWrapper);
      }
    }
  }

  private static class KafkaOffsetFetcher implements Callable<Boolean> {

    private static final int KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS = 10000;
    private final String _topicName;
    private final String _offsetCriteria;
    private final int _partitionId;

    private Exception _exception = null;
    private long _offset = -1;
    private PinotStreamConsumerFactory _pinotStreamConsumerFactory;
    KafkaStreamMetadata _kafkaStreamMetadata;

    private KafkaOffsetFetcher(final String offsetCriteria, int partitionId, KafkaStreamMetadata kafkaStreamMetadata) {
      _offsetCriteria = offsetCriteria;
      _partitionId = partitionId;
      _kafkaStreamMetadata = kafkaStreamMetadata;
      _pinotStreamConsumerFactory = _kafkaStreamMetadata.getPinotStreamConsumerFactory();
      _topicName = kafkaStreamMetadata.getStreamName();
    }

    private long getOffset() {
      return _offset;
    }

    private Exception getException() {
      return _exception;
    }

    @Override
    public Boolean call() throws Exception {

      PinotStreamConsumer
          kafkaConsumer = _pinotStreamConsumerFactory.buildConsumer("dummyClientId", _partitionId, _kafkaStreamMetadata);
      try {
        _offset = kafkaConsumer.fetchPartitionOffset(_offsetCriteria, KAFKA_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS);
        if (_exception != null) {
          LOGGER.info("Successfully retrieved offset({}) for kafka topic {} partition {}", _offset, _topicName, _partitionId);
        }
        return Boolean.TRUE;
      } catch (SimpleConsumerWrapper.TransientConsumerException e) {
        LOGGER.warn("Temporary exception when fetching offset for topic {} partition {}:{}", _topicName, _partitionId, e.getMessage());
        _exception = e;
        return Boolean.FALSE;
      } catch (Exception e) {
        _exception = e;
        throw e;
      } finally {
        org.apache.commons.io.IOUtils.closeQuietly(kafkaConsumer);
      }
    }
  }
}
