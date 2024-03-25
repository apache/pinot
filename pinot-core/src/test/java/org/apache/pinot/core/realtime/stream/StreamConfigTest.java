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
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.TimeUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class StreamConfigTest {

  /**
   * Checks that we fail if any of the mandatory properties are missing
   */
  @Test
  public void testStreamConfig() {
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    // test with empty map
    try {
      Map<String, String> streamConfigMap = new HashMap<>();
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    // All mandatory properties set
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);
    StreamConfig streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getType(), streamType);
    assertEquals(streamConfig.getTopicName(), topic);
    assertEquals(streamConfig.getConsumerFactoryClassName(), consumerFactoryClass);
    assertEquals(streamConfig.getDecoderClass(), decoderClass);

    // Missing streamType
    streamConfigMap.remove(StreamConfigProperties.STREAM_TYPE);
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    // Missing stream topic
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.remove(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME));
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }

    // Missing consumer factory - allowed
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.remove(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS));
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getConsumerFactoryClassName(), StreamConfig.DEFAULT_CONSUMER_FACTORY_CLASS_NAME_STRING);

    // Missing decoder class
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.remove(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS));
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (NullPointerException e) {
      // Expected
    }
  }

  /**
   * Checks that we use defaults where applicable
   */
  @Test
  public void testStreamConfigDefaults() {
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);

    // Mandatory values + defaults
    StreamConfig streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getType(), streamType);
    assertEquals(streamConfig.getTopicName(), topic);
    assertEquals(streamConfig.getConsumerFactoryClassName(), consumerFactoryClass);
    assertEquals(streamConfig.getDecoderClass(), decoderClass);
    assertEquals(streamConfig.getDecoderProperties().size(), 0);
    assertEquals(streamConfig.getOffsetCriteria(), new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest());
    assertEquals(streamConfig.getConnectionTimeoutMillis(), StreamConfig.DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);
    assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
    assertEquals(streamConfig.getFlushThresholdRows(), -1);
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), -1);

    String offsetCriteria = "smallest";
    String decoderProp1Key = "prop1";
    String decoderProp1Value = "decoderValueString";
    String connectionTimeout = "10";
    String fetchTimeout = "200";
    String flushThresholdTime = "2h";
    String flushThresholdRows = "500";
    String flushSegmentSize = "20M";
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.DECODER_PROPS_PREFIX) + "."
            + decoderProp1Key, decoderProp1Value);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_OFFSET_CRITERIA), offsetCriteria);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS), connectionTimeout);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        fetchTimeout);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, flushThresholdRows);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE, flushSegmentSize);

    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getType(), streamType);
    assertEquals(streamConfig.getTopicName(), topic);
    assertEquals(streamConfig.getConsumerFactoryClassName(), consumerFactoryClass);
    assertEquals(streamConfig.getDecoderClass(), decoderClass);
    assertEquals(streamConfig.getDecoderProperties().size(), 1);
    assertEquals(streamConfig.getDecoderProperties().get(decoderProp1Key), decoderProp1Value);
    assertTrue(streamConfig.getOffsetCriteria().isSmallest());
    assertEquals(streamConfig.getConnectionTimeoutMillis(), Long.parseLong(connectionTimeout));
    assertEquals(streamConfig.getFetchTimeoutMillis(), Integer.parseInt(fetchTimeout));
    assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));
    assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), DataSizeUtils.toBytes(flushSegmentSize));

    // Backward compatibility check for flushThresholdTime
    flushThresholdTime = "18000000";
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(), Long.parseLong(flushThresholdTime));

    // Backward compatibility check for flush threshold rows
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.put(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS, "10000");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdRows(), 10000);

    // Backward compatibility check for flush threshold segment size
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE);
    streamConfigMap.put(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE, "10M");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), DataSizeUtils.toBytes("10M"));
  }

  /**
   * Checks that we fail on invalid properties or use defaults
   */
  @Test
  public void testStreamConfigValidations() {
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    // All mandatory properties set
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);
    new StreamConfig(tableName, streamConfigMap);

    // Invalid fetch timeout
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "timeout");
    StreamConfig streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS);

    // Invalid connection timeout
    streamConfigMap.remove(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS));
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS), "timeout");
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getConnectionTimeoutMillis(), StreamConfig.DEFAULT_STREAM_CONNECTION_TIMEOUT_MILLIS);

    // Invalid flush threshold time
    streamConfigMap.remove(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONNECTION_TIMEOUT_MILLIS));
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, "time");
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME));
    }

    // Invalid flush threshold rows - deprecated property
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    streamConfigMap.put(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS, "rows");
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS));
    }

    // Invalid flush threshold rows - new property
    streamConfigMap.remove(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, "rows");
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS));
    }

    // Invalid flush segment size - deprecated property
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.put(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE, "size");
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE));
    }

    // Invalid flush segment size - new property
    streamConfigMap.remove(StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_DESIRED_SIZE);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE, "size");
    try {
      new StreamConfig(tableName, streamConfigMap);
      fail();
    } catch (IllegalArgumentException e) {
      // Expected
      assertTrue(e.getMessage().contains(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_SEGMENT_SIZE));
    }
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
    String consumerFactoryClass = FakeStreamConsumerFactory.class.getName();
    String decoderClass = FakeStreamMessageDecoder.class.getName();
    String flushThresholdRows = "200";
    String flushThresholdRowsLLC = "400";
    String flushThresholdTime = "2h";
    String flushThresholdTimeLLC = "4h";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);

    // Use default values if nothing provided
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(), StreamConfig.DEFAULT_FLUSH_THRESHOLD_TIME_MILLIS);
    assertEquals(streamConfig.getFlushThresholdRows(), -1);
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), -1);

    // Use regular values if provided
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS, flushThresholdRows);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME, flushThresholdTime);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));
    assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), -1);

    // Use regular values if both regular and llc config exists
    streamConfigMap.put(
        StreamConfigProperties.DEPRECATED_SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX,
        flushThresholdRowsLLC);
    streamConfigMap.put(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME + StreamConfigProperties.LLC_SUFFIX,
        flushThresholdTimeLLC);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTime));
    assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRows));
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), -1);

    // Use llc values if only llc config exists
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS);
    streamConfigMap.remove(StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME);
    streamConfig = new StreamConfig(tableName, streamConfigMap);
    assertEquals(streamConfig.getFlushThresholdTimeMillis(),
        (long) TimeUtils.convertPeriodToMillis(flushThresholdTimeLLC));
    assertEquals(streamConfig.getFlushThresholdRows(), Integer.parseInt(flushThresholdRowsLLC));
    assertEquals(streamConfig.getFlushThresholdSegmentSizeBytes(), -1);
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
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);

    String consumerType = "simple";
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        consumerType);
    new StreamConfig(tableName, streamConfigMap);

    consumerType = "lowlevel";
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        consumerType);
    new StreamConfig(tableName, streamConfigMap);

    try {
      consumerType = "highLevel";
      streamConfigMap.put(
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
          consumerType);
      new StreamConfig(tableName, streamConfigMap);
      fail("Invalid consumer type(s) " + consumerType + " in stream config");
    } catch (Exception e) {
      // expected
    }

    try {
      consumerType = "highLevel,simple";
      streamConfigMap.put(
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
          consumerType);
      new StreamConfig(tableName, streamConfigMap);
      fail("Invalid consumer type(s) " + consumerType + " in stream config");
    } catch (Exception e) {
      // expected
    }

    try {
      consumerType = "highLevel,lowlevel";
      streamConfigMap.put(
          StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
          consumerType);
      new StreamConfig(tableName, streamConfigMap);
      fail("Invalid consumer type(s) " + consumerType + " in stream config");
    } catch (Exception e) {
      // expected
    }
  }

  @Test
  public void testKinesisFetchTimeout() {
    String streamType = "fakeStream";
    String topic = "fakeTopic";
    String tableName = "fakeTable_REALTIME";
    String consumerFactoryClass = "KinesisConsumerFactory";
    String decoderClass = FakeStreamMessageDecoder.class.getName();

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClass);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        decoderClass);

    String consumerType = "simple";
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_CONSUMER_TYPES),
        consumerType);
    StreamConfig streamConfig = new StreamConfig(tableName, streamConfigMap);

    assertEquals(streamConfig.getFetchTimeoutMillis(), StreamConfig.DEFAULT_STREAM_FETCH_TIMEOUT_MILLIS_KINESIS);
  }
}
