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
package org.apache.pinot.core.realtime.stream;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConsumerFactory;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamMessageDecoder;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLevelStreamConfig;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class StreamConfigTest {

  /**
   * Checks that we fail if any of the mandatory properties are missing
   */
  @Test
  public void testStreamConfig() {
    boolean exception = false;
    StreamConfig streamConfig;
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerType = StreamConfig.ConsumerType.LOWLEVEL.toString();
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    // test with empty map
    try {
      Map<String, String> streamConfigMap = new HashMap<>();
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (NullPointerException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // All mandatory properties set
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);
    streamConfig = new StreamConfig(tableName, streamConfigMap);

    // Missing streamType
    streamConfigMap.remove(StreamConfigProperties.STREAM_TYPE);
    exception = false;
    try {
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (NullPointerException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // Missing stream topic
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .remove(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME));
    exception = false;
    try {
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (NullPointerException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // Missing consumer type
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap.remove(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES));
    exception = false;
    try {
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (NullPointerException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // Missing consumer factory
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.remove(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS));
    exception = false;
    try {
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (NullPointerException e) {
      exception = true;
    }
    Assert.assertFalse(exception);
    Assert.assertEquals(streamConfig.getConsumerFactoryClassName(),
        StreamConfig.DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING);

    // Missing decoder class
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap.remove(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS));
    exception = false;
    try {
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (NullPointerException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getType(), streamType);
    Assert.assertEquals(streamConfig.getTopicName(), topic);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertEquals(streamConfig.getConsumerFactoryClassName(), consumerFactoryClass);
    Assert.assertEquals(streamConfig.getDecoderClass(), decoderClass);
  }

  /**
   * Checks that we use defaults where applicable
   */
  @Test
  public void testStreamConfigDefaults() {
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String consumerType = "simple";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);

    // Mandatory values + defaults
    StreamConfig streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getType(), streamType);
    Assert.assertEquals(streamConfig.getTopicName(), topic);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertEquals(streamConfig.getConsumerFactoryClassName(), consumerFactoryClass);
    Assert.assertEquals(streamConfig.getDecoderClass(), decoderClass);
    Assert.assertEquals(streamConfig.getDecoderProperties().size(), 0);
    Assert
        .assertEquals(streamConfig.getOffsetCriteria(), new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest());
    Assert
        .assertEquals(streamConfig.getConnectionTimeoutMillis(), StreamConfig.DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);
    Assert.assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
    Assert.assertEquals(streamConfig.getFlushSegmentDesiredSizeBytes(),
        StreamConfig.DEFAULT_FLUSH_SEGMENT_DESIRED_SIZE_BYTES);

    consumerType = "lowLevel,highLevel";
    String offsetCriteria = "smallest";
    String decoderProp1Key = "prop1";
    String decoderProp1Value = "decoderValueString";
    String connectionTimeout = "10";
    String fetchTimeout = "200";
    String flushThresholdTime = "2h";
    String flushThresholdRows = "500";
    String flushSegmentSize = "20M";
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.DECODER_PROPS_PREFIX) + "."
            + decoderProp1Key, decoderProp1Value);
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), offsetCriteria);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS),
        connectionTimeout);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        fetchTimeout);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, flushThresholdRows);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, flushSegmentSize);

    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getType(), streamType);
    Assert.assertEquals(streamConfig.getTopicName(), topic);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(1), StreamConfig.ConsumerType.HIGHLEVEL);
    Assert.assertEquals(streamConfig.getConsumerFactoryClassName(), consumerFactoryClass);
    Assert.assertEquals(streamConfig.getDecoderClass(), decoderClass);
    Assert.assertEquals(streamConfig.getDecoderProperties().size(), 1);
    Assert.assertEquals(streamConfig.getDecoderProperties().get(decoderProp1Key), decoderProp1Value);
    Assert.assertTrue(streamConfig.getOffsetCriteria().isSmallest());
    Assert.assertEquals(streamConfig.getConnectionTimeoutMillis(), Long.parseLong(connectionTimeout));
    Assert.assertEquals(streamConfig.getFetchTimeoutMillis(), Integer.parseInt(fetchTimeout));
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));
    Assert.assertEquals(streamConfig.getFlushSegmentDesiredSizeBytes(), DataSizeUtils.toBytes(flushSegmentSize));

    // Backward compatibility check for flushThresholdTime
    flushThresholdTime = "18000000";
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), Long.parseLong(flushThresholdTime));
    flushThresholdTime = "invalid input";
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
  }

  /**
   * Checks that we fail on invalid properties or use defaults
   */
  @Test
  public void testStreamConfigValidations() {
    boolean exception;
    StreamConfig streamConfig;
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String consumerType = "simple";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    // All mandatory properties set
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);
    streamConfig = new StreamConfig(tableName, streamConfigMap);

    // Invalid consumer type
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            "invalidConsumerType");
    exception = false;
    try {
      streamConfig = new StreamConfig(tableName, streamConfigMap);
    } catch (IllegalArgumentException e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    // Invalid fetch timeout
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "timeout");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS);

    // Invalid connection timeout
    streamConfigMap.remove(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS));
    streamConfigMap.put(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS), "timeout");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert
        .assertEquals(streamConfig.getConnectionTimeoutMillis(), StreamConfig.DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);

    // Invalid flush threshold rows
    streamConfigMap.remove(StreamConfigProperties
        .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS));
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "rows");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);

    // Invalid flush threshold time
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, "time");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);

    // Invalid flush segment size
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_DESIRED_SIZE, "size");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushSegmentDesiredSizeBytes(),
        StreamConfig.DEFAULT_FLUSH_SEGMENT_DESIRED_SIZE_BYTES);
  }

  /**
   * Checks that we return the right flush threshold for regular vs llc configs
   */
  @Test
  public void testFlushThresholdStreamConfigs() {
    StreamConfig streamConfig;
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerType = "lowlevel";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();
    String flushThresholdRows = "200";
    String flushThresholdRowsLLC = "400";
    String flushThresholdTime = "2h";
    String flushThresholdTimeLLC = "4h";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);

    // use defaults if nothing provided
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);

    // use base values if provided
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, flushThresholdRows);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));

    // llc overrides provided, but base values will be picked in base StreamConfigs
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX,
        flushThresholdRowsLLC);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME + StreamConfigProperties.LLC_SUFFIX,
        flushThresholdTimeLLC);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));

    // llc overrides provided, no base values, defaults will be picked
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getFlushThresholdRows(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);
    Assert.assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);

    // PartitionLevel stream config will retrieve the llc overrides
    PartitionLevelStreamConfig partitionLevelStreamConfig = new PartitionLevelStreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(partitionLevelStreamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRowsLLC));
    Assert.assertEquals(partitionLevelStreamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTimeLLC));

    // PartitionLevelStreamConfig should use base values if llc overrides not provided
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX);
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME + StreamConfigProperties.LLC_SUFFIX);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, flushThresholdRows);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    partitionLevelStreamConfig = new PartitionLevelStreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(partitionLevelStreamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    Assert.assertEquals(partitionLevelStreamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));

    // PartitionLevelStreamConfig should use defaults if nothing provided
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    partitionLevelStreamConfig = new PartitionLevelStreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(partitionLevelStreamConfig.getFlushThresholdRows(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_ROWS);
    Assert.assertEquals(partitionLevelStreamConfig.getFlushThresholdTimeMillis(),
        StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
  }

  @Test
  public void testConsumerTypes() {
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME),
            topic);
    streamConfigMap.put(StreamConfigProperties
            .constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS),
        consumerFactoryClass);
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
            decoderClass);

    String consumerType = "simple";
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    StreamConfig streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertTrue(streamConfig.hasLowLevelConsumerType());
    Assert.assertFalse(streamConfig.hasHighLevelConsumerType());

    consumerType = "lowlevel";
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertTrue(streamConfig.hasLowLevelConsumerType());
    Assert.assertFalse(streamConfig.hasHighLevelConsumerType());

    consumerType = "highLevel";
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.HIGHLEVEL);
    Assert.assertFalse(streamConfig.hasLowLevelConsumerType());
    Assert.assertTrue(streamConfig.hasHighLevelConsumerType());

    consumerType = "highLevel,simple";
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.HIGHLEVEL);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(1), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertTrue(streamConfig.hasLowLevelConsumerType());
    Assert.assertTrue(streamConfig.hasHighLevelConsumerType());

    consumerType = "highLevel,lowlevel";
    streamConfigMap
        .put(StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
            consumerType);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(0), StreamConfig.ConsumerType.HIGHLEVEL);
    Assert.assertEquals(streamConfig.getConsumerTypes().get(1), StreamConfig.ConsumerType.LOWLEVEL);
    Assert.assertTrue(streamConfig.hasLowLevelConsumerType());
    Assert.assertTrue(streamConfig.hasHighLevelConsumerType());
  }
}
