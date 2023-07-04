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
package org.apache.pinot.spi.stream;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.EqualityUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides all the configs related to the stream as configured in the table config
 */
public class StreamConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamConfig.class);

  /**
   * The type of the stream consumer either HIGHLEVEL or LOWLEVEL. For backward compatibility, adding SIMPLE which is
   * equivalent to LOWLEVEL
   */
  public enum ConsumerType {
    HIGHLEVEL, LOWLEVEL
  }

  public static final int DEFAULT_FLUSH_THRESHOLD_ROWS = 5_000_000;
  public static final long DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS = TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS);
  public static final long DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES = 200 * 1024 * 1024; // 200M
  public static final int DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS = 100_000;
  public static final String DEFAULT_SERVER_UPLOAD_TO_DEEPSTORE = "false";

  public static final String DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING =
      "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory";

  public static final long DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS = 30_000;
  public static final int DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS = 5_000;
  public static final int DEFAULT_IDLE_TIMEOUT_MILLIS = 3 * 60 * 1000;

  private static final String SIMPLE_CONSUMER_TYPE_STRING = "simple";

  private static final double CONSUMPTION_RATE_LIMIT_NOT_SPECIFIED = -1;

  private final String _type;
  private final String _topicName;
  private final String _tableNameWithType;
  private final List<ConsumerType> _consumerTypes = new ArrayList<>();
  private final String _consumerFactoryClassName;
  private final String _decoderClass;
  private final Map<String, String> _decoderProperties = new HashMap<>();

  private final long _connectionTimeoutMillis;
  private final int _fetchTimeoutMillis;

  private final long _idleTimeoutMillis;

  private final int _flushThresholdRows;
  private final long _flushThresholdTimeMillis;
  private final long _flushThresholdSegmentSizeBytes;
  private final int _flushAutotuneInitialRows; // initial num rows to use for SegmentSizeBasedFlushThresholdUpdater

  private final String _groupId;

  private final double _topicConsumptionRateLimit;

  private final Map<String, String> _streamConfigMap = new HashMap<>();

  // Allow overriding it to use different offset criteria
  private OffsetCriteria _offsetCriteria;

  // Indicates if the segment should be uploaded to the deep store's file system or to the controller during the
  // segment commit protocol. By default, segment is uploaded to the controller during commit.
  // If this flag is set to true, the segment is uploaded to deep store.
  private final boolean _serverUploadToDeepStore;

  /**
   * Initializes a StreamConfig using the map of stream configs from the table config
   */
  public StreamConfig(String tableNameWithType, Map<String, String> streamConfigMap) {
    _type = streamConfigMap.get(StreamConfigProperties.STREAM_TYPE);
    Preconditions.checkNotNull(_type, "Stream type cannot be null");

    String topicNameKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_TOPIC_NAME);
    _topicName = streamConfigMap.get(topicNameKey);
    Preconditions.checkNotNull(_topicName, "Stream topic name " + topicNameKey + " cannot be null");

    _tableNameWithType = tableNameWithType;

    String consumerTypesKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONSUMER_TYPES);
    String consumerTypes = streamConfigMap.get(consumerTypesKey);
    Preconditions.checkNotNull(consumerTypes, "Must specify at least one consumer type " + consumerTypesKey);
    for (String consumerType : consumerTypes.split(",")) {
      if (consumerType.equals(
          SIMPLE_CONSUMER_TYPE_STRING)) { //For backward compatibility of stream configs which referred to lowlevel
        // as simple
        _consumerTypes.add(ConsumerType.LOWLEVEL);
        continue;
      }
      _consumerTypes.add(ConsumerType.valueOf(consumerType.toUpperCase()));
    }

    String consumerFactoryClassKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS);
    // For backward compatibility, default consumer factory is for Kafka.
    // TODO: remove this default and make it mandatory to have a factory class
    _consumerFactoryClassName =
        streamConfigMap.getOrDefault(consumerFactoryClassKey, DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING);

    String offsetCriteriaKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA);
    String offsetCriteriaValue = streamConfigMap.get(offsetCriteriaKey);
    if (offsetCriteriaValue != null) {
      _offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString(offsetCriteriaValue);
    } else {
      _offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest();
    }

    String decoderClassKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_DECODER_CLASS);
    _decoderClass = streamConfigMap.get(decoderClassKey);
    Preconditions.checkNotNull(_decoderClass, "Must specify decoder class name " + decoderClassKey);

    String streamDecoderPropPrefix =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.DECODER_PROPS_PREFIX);
    for (String key : streamConfigMap.keySet()) {
      if (key.startsWith(streamDecoderPropPrefix)) {
        _decoderProperties
            .put(StreamConfigProperties.getPropertySuffix(key, streamDecoderPropPrefix), streamConfigMap.get(key));
      }
    }

    long connectionTimeoutMillis = DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS;
    String connectionTimeoutKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS);
    String connectionTimeoutValue = streamConfigMap.get(connectionTimeoutKey);
    if (connectionTimeoutValue != null) {
      try {
        connectionTimeoutMillis = Long.parseLong(connectionTimeoutValue);
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", connectionTimeoutKey, connectionTimeoutValue,
            DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);
      }
    }
    _connectionTimeoutMillis = connectionTimeoutMillis;

    int fetchTimeoutMillis = DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS;
    String fetchTimeoutKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS);
    String fetchTimeoutValue = streamConfigMap.get(fetchTimeoutKey);
    if (fetchTimeoutValue != null) {
      try {
        fetchTimeoutMillis = Integer.parseInt(fetchTimeoutValue);
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", fetchTimeoutKey, fetchTimeoutValue,
            DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);
      }
    }
    _fetchTimeoutMillis = fetchTimeoutMillis;

    int idleTimeoutMillis = DEFAULT_IDLE_TIMEOUT_MILLIS;
    String idleTimeoutMillisKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_IDLE_TIMEOUT_MILLIS);
    String idleTimeoutMillisValue = streamConfigMap.get(idleTimeoutMillisKey);
    if (idleTimeoutMillisValue != null) {
      try {
        idleTimeoutMillis = Integer.parseInt(idleTimeoutMillisValue);
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", idleTimeoutMillisKey, idleTimeoutMillisValue,
            DEFAULT_IDLE_TIMEOUT_MILLIS);
      }
    }
    _idleTimeoutMillis = idleTimeoutMillis;

    _flushThresholdRows = extractFlushThresholdRows(streamConfigMap);
    _flushThresholdTimeMillis = extractFlushThresholdTimeMillis(streamConfigMap);
    _flushThresholdSegmentSizeBytes = extractFlushThresholdSegmentSize(streamConfigMap);
    _serverUploadToDeepStore = Boolean.parseBoolean(
        streamConfigMap.getOrDefault(StreamConfigProperties.SERVER_UPLOAD_TO_DEEPSTORE,
        DEFAULT_SERVER_UPLOAD_TO_DEEPSTORE));

    int autotuneInitialRows = 0;
    String initialRowsValue = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_AUTOTUNE_INITIAL_ROWS);
    if (initialRowsValue != null) {
      try {
        autotuneInitialRows = Integer.parseInt(initialRowsValue);
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}",
            StreamConfigProperties.SEGMENT_FLUSH_AUTOTUNE_INITIAL_ROWS, initialRowsValue,
            DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS);
      }
    }
    _flushAutotuneInitialRows = autotuneInitialRows > 0 ? autotuneInitialRows : DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS;

    String groupIdKey = StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.GROUP_ID);
    _groupId = streamConfigMap.get(groupIdKey);

    String rate = streamConfigMap.get(StreamConfigProperties.TOPIC_CONSUMPTION_RATE_LIMIT);
    _topicConsumptionRateLimit = rate != null ? Double.parseDouble(rate) : CONSUMPTION_RATE_LIMIT_NOT_SPECIFIED;

    _streamConfigMap.putAll(streamConfigMap);
  }

  public boolean isServerUploadToDeepStore() {
    return _serverUploadToDeepStore;
  }

  private long extractFlushThresholdSegmentSize(Map<String, String> streamConfigMap) {
    long segmentSizeBytes = -1;
    String key = StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE;
    String flushThresholdSegmentSizeStr = streamConfigMap.get(key);
    if (flushThresholdSegmentSizeStr == null) {
      // for backward compatibility with older property
      key = StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE;
      flushThresholdSegmentSizeStr = streamConfigMap.get(key);
    }

    if (flushThresholdSegmentSizeStr != null) {
      try {
        segmentSizeBytes = DataSizeUtils.toBytes(flushThresholdSegmentSizeStr);
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", key, flushThresholdSegmentSizeStr,
            DataSizeUtils.fromBytes(DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES));
      }
    }
    if (segmentSizeBytes > 0) {
      return segmentSizeBytes;
    } else {
      return DEFAULT_FLUSH_THRESHOLD_SEGMENT_SIZE_BYTES;
    }
  }

  protected int extractFlushThresholdRows(Map<String, String> streamConfigMap) {
    String key = StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS;
    String flushThresholdRowsStr = streamConfigMap.get(key);
    if (flushThresholdRowsStr == null) {
      // for backward compatibility with older property
      key = StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS;
      flushThresholdRowsStr = streamConfigMap.get(key);
    }
    if (flushThresholdRowsStr != null) {
      try {
        int flushThresholdRows = Integer.parseInt(flushThresholdRowsStr);
        // Flush threshold rows 0 means using segment size based flush threshold
        Preconditions.checkState(flushThresholdRows >= 0);
        return flushThresholdRows;
      } catch (Exception e) {
        LOGGER
            .warn("Invalid config {}: {}, defaulting to: {}", key, flushThresholdRowsStr, DEFAULT_FLUSH_THRESHOLD_ROWS);
        return DEFAULT_FLUSH_THRESHOLD_ROWS;
      }
    } else {
      return DEFAULT_FLUSH_THRESHOLD_ROWS;
    }
  }

  protected long extractFlushThresholdTimeMillis(Map<String, String> streamConfigMap) {
    String flushThresholdTimeStr = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    if (flushThresholdTimeStr != null) {
      try {
        return TimeUtils.convertPeriodToMillis(flushThresholdTimeStr);
      } catch (Exception e) {
        try {
          // For backward-compatibility, parse it as milliseconds value
          return Long.parseLong(flushThresholdTimeStr);
        } catch (NumberFormatException nfe) {
          LOGGER.warn("Invalid config {}: {}, defaulting to: {}", StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME,
              flushThresholdTimeStr, DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
          return DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS;
        }
      }
    } else {
      return DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS;
    }
  }

  public String getType() {
    return _type;
  }

  public String getTopicName() {
    return _topicName;
  }

  public List<ConsumerType> getConsumerTypes() {
    return _consumerTypes;
  }

  public boolean hasHighLevelConsumerType() {
    return _consumerTypes.contains(ConsumerType.HIGHLEVEL);
  }

  public boolean hasLowLevelConsumerType() {
    return _consumerTypes.contains(ConsumerType.LOWLEVEL);
  }

  public String getConsumerFactoryClassName() {
    return _consumerFactoryClassName;
  }

  public OffsetCriteria getOffsetCriteria() {
    return _offsetCriteria;
  }

  public void setOffsetCriteria(OffsetCriteria offsetCriteria) {
    _offsetCriteria = offsetCriteria;
  }

  public String getDecoderClass() {
    return _decoderClass;
  }

  public Map<String, String> getDecoderProperties() {
    return _decoderProperties;
  }

  public long getConnectionTimeoutMillis() {
    return _connectionTimeoutMillis;
  }

  public int getFetchTimeoutMillis() {
    return _fetchTimeoutMillis;
  }

  public long getIdleTimeoutMillis() {
    return _idleTimeoutMillis;
  }

  public int getFlushThresholdRows() {
    return _flushThresholdRows;
  }

  public long getFlushThresholdTimeMillis() {
    return _flushThresholdTimeMillis;
  }

  public long getFlushThresholdSegmentSizeBytes() {
    return _flushThresholdSegmentSizeBytes;
  }

  public int getFlushAutotuneInitialRows() {
    return _flushAutotuneInitialRows;
  }

  public String getGroupId() {
    return _groupId;
  }

  public Optional<Double> getTopicConsumptionRateLimit() {
    return _topicConsumptionRateLimit == CONSUMPTION_RATE_LIMIT_NOT_SPECIFIED ? Optional.empty()
        : Optional.of(_topicConsumptionRateLimit);
  }

  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public Map<String, String> getStreamConfigsMap() {
    return _streamConfigMap;
  }

  @Override
  public String toString() {
    return "StreamConfig{" + "_type='" + _type + '\'' + ", _topicName='" + _topicName + '\'' + ", _consumerTypes="
        + _consumerTypes + ", _consumerFactoryClassName='" + _consumerFactoryClassName + '\'' + ", _offsetCriteria='"
        + _offsetCriteria + '\'' + ", _connectionTimeoutMillis=" + _connectionTimeoutMillis + ", _fetchTimeoutMillis="
        + _fetchTimeoutMillis + ", _idleTimeoutMillis=" + _idleTimeoutMillis + ", _flushThresholdRows="
        + _flushThresholdRows + ", _flushThresholdTimeMillis=" + _flushThresholdTimeMillis
        + ", _flushSegmentDesiredSizeBytes=" + _flushThresholdSegmentSizeBytes + ", _flushAutotuneInitialRows="
        + _flushAutotuneInitialRows + ", _decoderClass='" + _decoderClass + '\'' + ", _decoderProperties="
        + _decoderProperties + ", _groupId='" + _groupId + "', _topicConsumptionRateLimit=" + _topicConsumptionRateLimit
        + ", _tableNameWithType='" + _tableNameWithType + ", _serverUploadToDeepStore=" + _serverUploadToDeepStore
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (EqualityUtils.isSameReference(this, o)) {
      return true;
    }

    if (EqualityUtils.isNullOrNotSameClass(this, o)) {
      return false;
    }

    StreamConfig that = (StreamConfig) o;

    return EqualityUtils.isEqual(_connectionTimeoutMillis, that._connectionTimeoutMillis) && EqualityUtils.isEqual(
        _fetchTimeoutMillis, that._fetchTimeoutMillis) && EqualityUtils.isEqual(_idleTimeoutMillis,
        that._idleTimeoutMillis) && EqualityUtils.isEqual(_flushThresholdRows, that._flushThresholdRows)
        && EqualityUtils.isEqual(_flushThresholdTimeMillis, that._flushThresholdTimeMillis) && EqualityUtils.isEqual(
        _flushThresholdSegmentSizeBytes, that._flushThresholdSegmentSizeBytes) && EqualityUtils.isEqual(
        _flushAutotuneInitialRows, that._flushAutotuneInitialRows) && EqualityUtils.isEqual(_type, that._type)
        && EqualityUtils.isEqual(_topicName, that._topicName) && EqualityUtils.isEqual(_consumerTypes,
        that._consumerTypes) && EqualityUtils.isEqual(_consumerFactoryClassName, that._consumerFactoryClassName)
        && EqualityUtils.isEqual(_offsetCriteria, that._offsetCriteria) && EqualityUtils.isEqual(_decoderClass,
        that._decoderClass) && EqualityUtils.isEqual(_decoderProperties, that._decoderProperties)
        && EqualityUtils.isEqual(_groupId, that._groupId) && EqualityUtils.isEqual(_tableNameWithType,
        that._tableNameWithType) && EqualityUtils.isEqual(_topicConsumptionRateLimit, that._topicConsumptionRateLimit)
        && EqualityUtils.isEqual(_streamConfigMap, that._streamConfigMap)
        && _serverUploadToDeepStore == that._serverUploadToDeepStore;
  }

  @Override
  public int hashCode() {
    int result = EqualityUtils.hashCodeOf(_type);
    result = EqualityUtils.hashCodeOf(result, _topicName);
    result = EqualityUtils.hashCodeOf(result, _consumerTypes);
    result = EqualityUtils.hashCodeOf(result, _consumerFactoryClassName);
    result = EqualityUtils.hashCodeOf(result, _offsetCriteria);
    result = EqualityUtils.hashCodeOf(result, _connectionTimeoutMillis);
    result = EqualityUtils.hashCodeOf(result, _fetchTimeoutMillis);
    result = EqualityUtils.hashCodeOf(result, _idleTimeoutMillis);
    result = EqualityUtils.hashCodeOf(result, _flushThresholdRows);
    result = EqualityUtils.hashCodeOf(result, _flushThresholdTimeMillis);
    result = EqualityUtils.hashCodeOf(result, _flushThresholdSegmentSizeBytes);
    result = EqualityUtils.hashCodeOf(result, _flushAutotuneInitialRows);
    result = EqualityUtils.hashCodeOf(result, _decoderClass);
    result = EqualityUtils.hashCodeOf(result, _decoderProperties);
    result = EqualityUtils.hashCodeOf(result, _groupId);
    result = EqualityUtils.hashCodeOf(result, _topicConsumptionRateLimit);
    result = EqualityUtils.hashCodeOf(result, _streamConfigMap);
    result = EqualityUtils.hashCodeOf(result, _tableNameWithType);
    result = EqualityUtils.hashCodeOf(result, _serverUploadToDeepStore);
    return result;
  }
}
