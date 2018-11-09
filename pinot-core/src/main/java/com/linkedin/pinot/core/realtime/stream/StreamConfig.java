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
import com.linkedin.pinot.common.utils.DataSize;
import com.linkedin.pinot.common.utils.EqualityUtils;
import com.linkedin.pinot.common.utils.time.TimeUtils;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaConsumerFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

  private static final int DEFAULT_FLUSH_THRESHOLD_ROWS = 5_000_000;
  private static final long DEFAULT_FLUSH_THRESHOLD_TIME = TimeUnit.MILLISECONDS.convert(6, TimeUnit.HOURS);
  private static final long DEFAULT_DESIRED_SEGMENT_SIZE_BYTES = 200 * 1024 * 1024; // 200M
  private static final String DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING = KafkaConsumerFactory.class.getName();

  protected static final long DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS = 30_000;
  protected static final int DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS = 5_000;
  protected static final String SIMPLE_CONSUMER_TYPE_STRING = "simple";

  final private String _type;
  final private String _topicName;
  final private List<ConsumerType> _consumerTypes = new ArrayList<>();
  final private String _consumerFactoryClassName;
  final private OffsetCriteria _offsetCriteria;
  final private String _decoderClass;
  final private Map<String, String> _decoderProperties = new HashMap<>();

  final private long _connectionTimeoutMillis;
  final private int _fetchTimeoutMillis;

  final private int _flushThresholdRows;
  final private long _flushThresholdTimeMillis;
  final private long _flushSegmentDesiredSizeBytes;

  final private String _groupId;

  final private Map<String, String> _streamConfigMap = new HashMap<>();

  /**
   * Initializes a StreamConfig using the map of stream configs from the table config
   * @param streamConfigMap
   */
  public StreamConfig(Map<String, String> streamConfigMap) {

    _type = streamConfigMap.get(StreamConfigProperties.STREAM_TYPE);
    Preconditions.checkNotNull(_type, "Stream type cannot be null");

    String topicNameKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_TOPIC_NAME);
    _topicName = streamConfigMap.get(topicNameKey);
    Preconditions.checkNotNull(_topicName, "Stream topic name " + topicNameKey + " cannot be null");

    String consumerTypesKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONSUMER_TYPES);
    String consumerTypes = streamConfigMap.get(consumerTypesKey);
    Preconditions.checkNotNull(consumerTypes, "Must specify at least one consumer type " + consumerTypesKey);
    for (String consumerType : consumerTypes.split(",")) {
      if (consumerType.equals(SIMPLE_CONSUMER_TYPE_STRING)) { //For backward compatibility of stream configs which referred to lowlevel as simple
        consumerType = ConsumerType.LOWLEVEL.toString();
      }
      _consumerTypes.add(ConsumerType.valueOf(consumerType.toUpperCase()));
    }

    String consumerFactoryClassKey =
        StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS);
    if (streamConfigMap.containsKey(consumerFactoryClassKey)) {
      _consumerFactoryClassName = streamConfigMap.get(consumerFactoryClassKey);
    } else {
      // For backward compatibility, default consumer factory is for Kafka.
      _consumerFactoryClassName = DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING;
    }
    LOGGER.info("Stream type: {}, name: {}, consumer types: {}, consumer factory: {}", _type, _topicName,
        _consumerTypes, _consumerFactoryClassName);

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
        _decoderProperties.put(StreamConfigProperties.getPropertySuffix(key, streamDecoderPropPrefix),
            streamConfigMap.get(key));
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
        LOGGER.warn("Caught exception while parsing the connection timeout, defaulting to {} ms", e,
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
        LOGGER.warn("Caught exception while parsing the fetch timeout, defaulting to {} ms",
            DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS, e);
      }
    }
    _fetchTimeoutMillis = fetchTimeoutMillis;

    int flushThresholdRows = DEFAULT_FLUSH_THRESHOLD_ROWS;
    String flushThresholdRowsValue = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    if (flushThresholdRowsValue != null) {
      try {
        flushThresholdRows = Integer.parseInt(flushThresholdRowsValue);
      } catch (Exception e) {
        LOGGER.warn("Caught exception when parsing flush threshold rows {}:{}, defaulting to {}",
            StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, flushThresholdRowsValue, DEFAULT_FLUSH_THRESHOLD_ROWS,
            e);
      }
    }
    _flushThresholdRows = flushThresholdRows;

    long flushThresholdTime = DEFAULT_FLUSH_THRESHOLD_TIME;
    String flushThresholdTimeValue = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    if (flushThresholdTimeValue != null) {
      try {
        flushThresholdTime = TimeUtils.convertPeriodToMillis(flushThresholdTimeValue);
      } catch (Exception e) {
        try {
          // For backward compatibility, default is using milliseconds value.
          flushThresholdTime = Long.parseLong(flushThresholdTimeValue);
        } catch (Exception e1) {
          LOGGER.warn("Caught exception when converting flush threshold period to millis {}:{}, defaulting to {}",
              StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTimeValue, DEFAULT_FLUSH_THRESHOLD_TIME,
              e);
        }
      }
    }
    _flushThresholdTimeMillis = flushThresholdTime;

    long flushDesiredSize = -1;
    String flushSegmentDesiredSizeValue = streamConfigMap.get(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE);
    if (flushSegmentDesiredSizeValue != null) {
      flushDesiredSize = DataSize.toBytes(flushSegmentDesiredSizeValue);
    }
    if (flushDesiredSize > 0) {
      _flushSegmentDesiredSizeBytes = flushDesiredSize;
    } else {
      _flushSegmentDesiredSizeBytes = DEFAULT_DESIRED_SEGMENT_SIZE_BYTES;
    }

    String groupIdKey = StreamConfigProperties.constructStreamProperty(_type, StreamConfigProperties.GROUP_ID);
    _groupId = streamConfigMap.get(groupIdKey);


    _streamConfigMap.putAll(streamConfigMap);
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

  public long getConnectionTimeoutMillis() {
    return _connectionTimeoutMillis;
  }

  public int getFetchTimeoutMillis() {
    return _fetchTimeoutMillis;
  }

  public int getFlushThresholdRows() {
    return _flushThresholdRows;
  }

  public static int getDefaultFlushThresholdRows() {
    return DEFAULT_FLUSH_THRESHOLD_ROWS;
  }

  public long getFlushThresholdTimeMillis() {
    return _flushThresholdTimeMillis;
  }

  public static long getDefaultFlushThresholdTimeMillis() {
    return DEFAULT_FLUSH_THRESHOLD_TIME;
  }

  public static String getDefaultConsumerFactoryClassName() {
    return DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING;
  }

  public long getFlushSegmentDesiredSizeBytes() {
    return _flushSegmentDesiredSizeBytes;
  }

  public static long getDefaultDesiredSegmentSizeBytes() {
    return DEFAULT_DESIRED_SEGMENT_SIZE_BYTES;
  }

  public String getDecoderClass() {
    return _decoderClass;
  }

  public Map<String, String> getDecoderProperties() {
    return _decoderProperties;
  }

  public String getGroupId() {
    return _groupId;
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
        + ", _decoderClass='" + _decoderClass + '\'' + ", _decoderProperties=" + _decoderProperties
        + ", _groupId='" + _groupId + '}';
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
        _fetchTimeoutMillis, that._fetchTimeoutMillis) && EqualityUtils.isEqual(_flushThresholdRows,
        that._flushThresholdRows) && EqualityUtils.isEqual(_flushThresholdTimeMillis, that._flushThresholdTimeMillis)
        && EqualityUtils.isEqual(_flushSegmentDesiredSizeBytes, that._flushSegmentDesiredSizeBytes)
        && EqualityUtils.isEqual(_type, that._type) && EqualityUtils.isEqual(_topicName, that._topicName)
        && EqualityUtils.isEqual(_consumerTypes, that._consumerTypes) && EqualityUtils.isEqual(
        _consumerFactoryClassName, that._consumerFactoryClassName) && EqualityUtils.isEqual(_offsetCriteria,
        that._offsetCriteria) && EqualityUtils.isEqual(_decoderClass, that._decoderClass) && EqualityUtils.isEqual(
        _decoderProperties, that._decoderProperties) && EqualityUtils.isEqual(_groupId, that._groupId)
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
    result = EqualityUtils.hashCodeOf(result, _decoderClass);
    result = EqualityUtils.hashCodeOf(result, _decoderProperties);
    result = EqualityUtils.hashCodeOf(result, _groupId);
    result = EqualityUtils.hashCodeOf(result, _streamConfigMap);
    return result;
  }
}
