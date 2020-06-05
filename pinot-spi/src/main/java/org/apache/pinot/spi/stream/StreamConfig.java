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
   * The type of the stream consumer either HIGHLEVEL or LOWLEVEL. For backward compatibility, adding SIMPLE which is equivalent to LOWLEVEL
   */
  public enum ConsumerType {
    HIGHLEVEL, LOWLEVEL
  }

  public static final int DEFAULT_FLUSH_THRESHOLD_ROWS = 5_000_000;
  public static final long DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS = TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS);
  public static final long DEFAULT_FLUSH_SEGMENT_DESIRED_SIZE_BYTES = 200 * 1024 * 1024; // 200M
  public static final int DEFAULT_FLUSH_AUTOTUNE_INITIAL_ROWS = 100_000;

  public static final String DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING =
      "org.apache.pinot.plugin.stream.kafka09.KafkaConsumerFactory";
  public static final String DEFAULT_PARTITION_OFFSET_FACTORY_CLASS_NAME_STRING = LongMsgOffsetFactory.class.getName();

  public static final long DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS = 30_000;
  public static final int DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS = 5_000;

  private static final String SIMPLE_CONSUMER_TYPE_STRING = "simple";

  private final String _type;
  private final String _topicName;
  private final String _tableNameWithType;
  private final List<ConsumerType> _consumerTypes = new ArrayList<>();
  private final String _consumerFactoryClassName;
  private final OffsetCriteria _offsetCriteria;
  private final String _decoderClass;
  private final Map<String, String> _decoderProperties = new HashMap<>();
  private final String _partitionOffsetFactoryClassName;

  private final long _connectionTimeoutMillis;
  private final int _fetchTimeoutMillis;

  private final int _flushThresholdRows;
  private final long _flushThresholdTimeMillis;
  private final long _flushSegmentDesiredSizeBytes;
  private final int _flushAutotuneInitialRows; // initial num rows to use for SegmentSizeBasedFlushThresholdUpdater

  private final String _groupId;

  private final Map<String, String> _streamConfigMap = new HashMap<>();

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
          SIMPLE_CONSUMER_TYPE_STRING)) { //For backward compatibility of stream configs which referred to lowlevel as simple
        consumerType = ConsumerType.LOWLEVEL.toString();
      }
      _consumerTypes.add(ConsumerType.valueOf(consumerType.toUpperCase()));
    }

    String consumerFactoryClassKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS);
    // For backward compatibility, default consumer factory is for Kafka.
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

    String partitionOffsetClassKey = StreamConfigProperties.constructStreamProperty(_type,
        StreamConfigProperties.PARTITION_MSG_OFFSET_FACTORY_CLASS);
    // For backward compatibility, the offset factory class is for handling kafka offsets (long type)
    _partitionOffsetFactoryClassName = streamConfigMap.getOrDefault(partitionOffsetClassKey,
        DEFAULT_PARTITION_OFFSET_FACTORY_CLASS_NAME_STRING);

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

    _flushThresholdRows = extractFlushThresholdRows(streamConfigMap);
    _flushThresholdTimeMillis = extractFlushThresholdTimeMillis(streamConfigMap);

    long flushDesiredSize = -1;
    String flushSegmentDesiredSizeValue = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE);
    if (flushSegmentDesiredSizeValue != null) {
      try {
        flushDesiredSize = DataSizeUtils.toBytes(flushSegmentDesiredSizeValue);
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE,
            flushSegmentDesiredSizeValue, DataSizeUtils.fromBytes(DEFAULT_FLUSH_SEGMENT_DESIRED_SIZE_BYTES));
      }
    }
    if (flushDesiredSize > 0) {
      _flushSegmentDesiredSizeBytes = flushDesiredSize;
    } else {
      _flushSegmentDesiredSizeBytes = DEFAULT_FLUSH_SEGMENT_DESIRED_SIZE_BYTES;
    }

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

    _streamConfigMap.putAll(streamConfigMap);
  }

  protected int extractFlushThresholdRows(Map<String, String> streamConfigMap) {
    String flushThresholdRowsStr = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    if (flushThresholdRowsStr != null) {
      try {
        int flushThresholdRows = Integer.parseInt(flushThresholdRowsStr);
        // Flush threshold rows 0 means using segment size based flush threshold
        Preconditions.checkState(flushThresholdRows >= 0);
        return flushThresholdRows;
      } catch (Exception e) {
        LOGGER.warn("Invalid config {}: {}, defaulting to: {}", StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS,
            flushThresholdRowsStr, DEFAULT_FLUSH_THRESHOLD_ROWS);
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

  public String getPartitionOffsetFactoryClassName() {
    return _partitionOffsetFactoryClassName;
  }

  public OffsetCriteria getOffsetCriteria() {
    return _offsetCriteria;
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

  public int getFlushThresholdRows() {
    return _flushThresholdRows;
  }

  public long getFlushThresholdTimeMillis() {
    return _flushThresholdTimeMillis;
  }

  public long getFlushSegmentDesiredSizeBytes() {
    return _flushSegmentDesiredSizeBytes;
  }

  public int getFlushAutotuneInitialRows() {
    return _flushAutotuneInitialRows;
  }

  public String getGroupId() {
    return _groupId;
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
        + _fetchTimeoutMillis + ", _flushThresholdRows=" + _flushThresholdRows + ", _flushThresholdTimeMillis="
        + _flushThresholdTimeMillis + ", _flushSegmentDesiredSizeBytes=" + _flushSegmentDesiredSizeBytes
        + ", _flushAutotuneInitialRows=" + _flushAutotuneInitialRows + ", _decoderClass='" + _decoderClass + '\''
        + ", _decoderProperties=" + _decoderProperties + ", _groupId='" + _groupId + ", _tableNameWithType='"
        + _tableNameWithType + '}';
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

    return EqualityUtils.isEqual(_connectionTimeoutMillis, that._connectionTimeoutMillis) && EqualityUtils
        .isEqual(_fetchTimeoutMillis, that._fetchTimeoutMillis) && EqualityUtils
        .isEqual(_flushThresholdRows, that._flushThresholdRows) && EqualityUtils
        .isEqual(_flushThresholdTimeMillis, that._flushThresholdTimeMillis) && EqualityUtils
        .isEqual(_flushSegmentDesiredSizeBytes, that._flushSegmentDesiredSizeBytes) && EqualityUtils
        .isEqual(_flushAutotuneInitialRows, that._flushAutotuneInitialRows) && EqualityUtils.isEqual(_type, that._type)
        && EqualityUtils.isEqual(_topicName, that._topicName) && EqualityUtils
        .isEqual(_consumerTypes, that._consumerTypes) && EqualityUtils
        .isEqual(_consumerFactoryClassName, that._consumerFactoryClassName) && EqualityUtils
        .isEqual(_offsetCriteria, that._offsetCriteria) && EqualityUtils.isEqual(_decoderClass, that._decoderClass)
        && EqualityUtils.isEqual(_decoderProperties, that._decoderProperties) && EqualityUtils
        .isEqual(_groupId, that._groupId) && EqualityUtils.isEqual(_tableNameWithType, that._tableNameWithType)
        && EqualityUtils.isEqual(_streamConfigMap, that._streamConfigMap);
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
    result = EqualityUtils.hashCodeOf(result, _flushThresholdRows);
    result = EqualityUtils.hashCodeOf(result, _flushThresholdTimeMillis);
    result = EqualityUtils.hashCodeOf(result, _flushSegmentDesiredSizeBytes);
    result = EqualityUtils.hashCodeOf(result, _flushAutotuneInitialRows);
    result = EqualityUtils.hashCodeOf(result, _decoderClass);
    result = EqualityUtils.hashCodeOf(result, _decoderProperties);
    result = EqualityUtils.hashCodeOf(result, _groupId);
    result = EqualityUtils.hashCodeOf(result, _streamConfigMap);
    result = EqualityUtils.hashCodeOf(result, _tableNameWithType);
    return result;
  }
}
