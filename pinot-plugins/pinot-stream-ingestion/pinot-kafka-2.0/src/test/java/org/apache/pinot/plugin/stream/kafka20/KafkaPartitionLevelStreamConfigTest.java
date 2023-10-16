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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class KafkaPartitionLevelStreamConfigTest {
  private static final String KAFKA_DECODER_CLASS_NAME =
      "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder";

  private KafkaPartitionLevelStreamConfig getStreamConfig(String topic, String bootstrapHosts, String buffer,
      String socketTimeout) {
    return getStreamConfig(topic, bootstrapHosts, buffer, socketTimeout, null, null, null);
  }

  private KafkaPartitionLevelStreamConfig getStreamConfig(String topic, String bootstrapHosts, String buffer,
      String socketTimeout, String isolationLevel) {
    return getStreamConfig(topic, bootstrapHosts, buffer, socketTimeout, null, null, isolationLevel);
  }

  private KafkaPartitionLevelStreamConfig getStreamConfig(String topic, String bootstrapHosts, String buffer,
      String socketTimeout, String fetcherSize, String fetcherMinBytes, String isolationLevel) {
    return getStreamConfig(topic, bootstrapHosts, buffer, socketTimeout, fetcherSize, fetcherMinBytes, isolationLevel,
        null);
  }

  private KafkaPartitionLevelStreamConfig getStreamConfig(String topic, String bootstrapHosts, String buffer,
      String socketTimeout, String fetcherSize, String fetcherMinBytes, String isolationLevel,
      String populateRowMetadata) {
    Map<String, String> streamConfigMap = new HashMap<>();
    String streamType = "kafka";
    String consumerFactoryClassName = KafkaConsumerFactory.class.getName();
    String tableNameWithType = "tableName_REALTIME";
    streamConfigMap.put(StreamConfigProperties.STREAM_TYPE, streamType);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_TOPIC_NAME), topic);
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(streamType,
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), consumerFactoryClassName);
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty(streamType, StreamConfigProperties.STREAM_DECODER_CLASS),
        KAFKA_DECODER_CLASS_NAME);
    streamConfigMap.put("stream.kafka.broker.list", bootstrapHosts);
    if (buffer != null) {
      streamConfigMap.put("stream.kafka.buffer.size", buffer);
    }
    if (socketTimeout != null) {
      streamConfigMap.put("stream.kafka.socket.timeout", socketTimeout);
    }
    if (fetcherSize != null) {
      streamConfigMap.put("stream.kafka.fetcher.size", fetcherSize);
    }
    if (fetcherMinBytes != null) {
      streamConfigMap.put("stream.kafka.fetcher.minBytes", fetcherMinBytes);
    }
    if (isolationLevel != null) {
      streamConfigMap.put("stream.kafka.isolation.level", isolationLevel);
    }
    if (populateRowMetadata != null) {
      streamConfigMap.put("stream.kafka.metadata.populate", populateRowMetadata);
    }
    return new KafkaPartitionLevelStreamConfig(new StreamConfig(tableNameWithType, streamConfigMap));
  }

  @Test
  public void testGetKafkaIsolationLevel() {
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "", "", "", "read_committed");
    Assert.assertEquals("read_committed", config.getKafkaIsolationLevel());
  }

  @Test
  public void testGetKafkaTopicName() {
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "", "", "");
    Assert.assertEquals("topic", config.getKafkaTopicName());
  }

  @Test
  public void testGetBootstrapHosts() {
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "host1", "", "");
    Assert.assertEquals("host1", config.getBootstrapHosts());
  }

  @Test
  public void testGetKafkaBufferSize() {
    // test default
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "host1", null, "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        config.getKafkaBufferSize());

    config = getStreamConfig("topic", "host1", "", "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        config.getKafkaBufferSize());

    config = getStreamConfig("topic", "host1", "bad value", "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        config.getKafkaBufferSize());

    // correct config
    config = getStreamConfig("topic", "host1", "100", "");
    Assert.assertEquals(100, config.getKafkaBufferSize());
  }

  @Test
  public void testGetKafkaSocketTimeout() {
    // test default
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "host1", "", null);
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        config.getKafkaSocketTimeout());

    config = getStreamConfig("topic", "host1", "", "");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        config.getKafkaSocketTimeout());

    config = getStreamConfig("topic", "host1", "", "bad value");
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        config.getKafkaSocketTimeout());

    // correct config
    config = getStreamConfig("topic", "host1", "", "100");
    Assert.assertEquals(100, config.getKafkaSocketTimeout());
  }

  @Test
  public void testGetFetcherSize() {
    // test default
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "host1", "", "", "", null, null);
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        config.getKafkaFetcherSizeBytes());

    config = getStreamConfig("topic", "host1", "100", "", "", null, null);
    Assert.assertEquals(100, config.getKafkaFetcherSizeBytes());

    config = getStreamConfig("topic", "host1", "100", "", "bad value", null, null);
    Assert.assertEquals(100, config.getKafkaFetcherSizeBytes());

    // correct config
    config = getStreamConfig("topic", "host1", "100", "", "200", null, null);
    Assert.assertEquals(200, config.getKafkaFetcherSizeBytes());
  }

  @Test
  public void testGetFetcherMinBytes() {
    // test default
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "host1", "", "", "", null, null);
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_FETCHER_MIN_BYTES_DEFAULT,
        config.getKafkaFetcherMinBytes());

    config = getStreamConfig("topic", "host1", "", "", "", "", null);
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_FETCHER_MIN_BYTES_DEFAULT,
        config.getKafkaFetcherMinBytes());

    config = getStreamConfig("topic", "host1", "", "", "", "bad value", null);
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_FETCHER_MIN_BYTES_DEFAULT,
        config.getKafkaFetcherMinBytes());

    // correct config
    config = getStreamConfig("topic", "host1", "", "", "", "100", null);
    Assert.assertEquals(100, config.getKafkaFetcherMinBytes());
  }

  @Test
  public void testIsPopulateRowMetadata() {
    // test default
    KafkaPartitionLevelStreamConfig config = getStreamConfig("topic", "host1", null, null, null, null, null, null);
    Assert.assertFalse(config.isPopulateMetadata());

    config = getStreamConfig("topic", "host1", null, null, null, null, null, "bad value");
    Assert.assertFalse(config.isPopulateMetadata());

    config = getStreamConfig("topic", "host1", null, null, null, null, null, "TrUe");
    Assert.assertTrue(config.isPopulateMetadata());
  }
}
