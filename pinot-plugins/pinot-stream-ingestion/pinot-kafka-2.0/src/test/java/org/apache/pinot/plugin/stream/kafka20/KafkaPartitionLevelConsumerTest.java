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
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.plugin.stream.kafka20.utils.MiniKafkaCluster;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the KafkaPartitionLevelConsumer.
 */
public class KafkaPartitionLevelConsumerTest {
  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC_1 = "foo";
  private static final String TEST_TOPIC_2 = "bar";
  private static final int NUM_MSG_PRODUCED_PER_PARTITION = 1000;

  private MiniKafkaCluster _kafkaCluster;
  private String _kafkaBrokerAddress;

  @BeforeClass
  public void setUp()
      throws Exception {
    _kafkaCluster = new MiniKafkaCluster("0");
    _kafkaCluster.start();
    _kafkaBrokerAddress = _kafkaCluster.getKafkaServerAddress();
    _kafkaCluster.createTopic(TEST_TOPIC_1, 1, 1);
    _kafkaCluster.createTopic(TEST_TOPIC_2, 2, 1);
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
    produceMsgToKafka();
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
  }

  private void produceMsgToKafka() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokerAddress);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < NUM_MSG_PRODUCED_PER_PARTITION; i++) {
        producer.send(new ProducerRecord<>(TEST_TOPIC_1, "sample_msg_" + i));
        // TEST_TOPIC_2 has 2 partitions
        producer.send(new ProducerRecord<>(TEST_TOPIC_2, "sample_msg_" + i));
        producer.send(new ProducerRecord<>(TEST_TOPIC_2, "sample_msg_" + i));
      }
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _kafkaCluster.deleteTopic(TEST_TOPIC_1);
    _kafkaCluster.deleteTopic(TEST_TOPIC_2);
    _kafkaCluster.close();
  }

  @Test
  public void testBuildConsumer()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = _kafkaBrokerAddress;
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfigMap.put("stream.kafka.fetcher.size", "10000");
    streamConfigMap.put("stream.kafka.fetcher.minBytes", "20000");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    // test default value
    KafkaPartitionLevelConsumer kafkaSimpleStreamConsumer = new KafkaPartitionLevelConsumer(clientId, streamConfig, 0);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), new LongMsgOffset(23456L), 10000);

    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaBufferSize());
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaSocketTimeout());

    // test parsing values
    Assert
        .assertEquals(10000, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaFetcherSizeBytes());
    Assert
        .assertEquals(20000, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaFetcherMinBytes());

    // test user defined values
    streamConfigMap.put("stream.kafka.buffer.size", "100");
    streamConfigMap.put("stream.kafka.socket.timeout", "1000");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
    kafkaSimpleStreamConsumer = new KafkaPartitionLevelConsumer(clientId, streamConfig, 0);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), new LongMsgOffset(23456L), 10000);
    Assert.assertEquals(100, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaBufferSize());
    Assert.assertEquals(1000, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaSocketTimeout());
  }

  @Test
  public void testGetPartitionCount() {
    String streamType = "kafka";
    String streamKafkaBrokerList = _kafkaBrokerAddress;
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC_1);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    KafkaStreamMetadataProvider streamMetadataProvider = new KafkaStreamMetadataProvider(clientId, streamConfig);
    Assert.assertEquals(streamMetadataProvider.fetchPartitionCount(10000L), 1);

    streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC_2);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    streamMetadataProvider = new KafkaStreamMetadataProvider(clientId, streamConfig);
    Assert.assertEquals(streamMetadataProvider.fetchPartitionCount(10000L), 2);
  }

  @Test
  public void testFetchMessages()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = _kafkaBrokerAddress;
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    int partition = 0;
    KafkaPartitionLevelConsumer kafkaSimpleStreamConsumer =
        new KafkaPartitionLevelConsumer(clientId, streamConfig, partition);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), new LongMsgOffset(23456L), 10000);
  }

  @Test
  public void testFetchOffsets()
      throws Exception {
    testFetchOffsets(TEST_TOPIC_1);
    testFetchOffsets(TEST_TOPIC_2);
  }

  private void testFetchOffsets(String topic)
      throws Exception {
    String streamType = "kafka";
    String streamKafkaBrokerList = _kafkaBrokerAddress;
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", topic);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    int numPartitions = new KafkaStreamMetadataProvider(clientId, streamConfig).fetchPartitionCount(10000);
    for (int partition = 0; partition < numPartitions; partition++) {
      KafkaStreamMetadataProvider kafkaStreamMetadataProvider =
          new KafkaStreamMetadataProvider(clientId, streamConfig, partition);
      Assert.assertEquals(new LongMsgOffset(0).compareTo(kafkaStreamMetadataProvider
          .fetchStreamPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000)), 0);
      Assert.assertEquals(new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION).compareTo(kafkaStreamMetadataProvider
          .fetchStreamPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest(), 10000)), 0);
    }
  }

  @Test
  public void testConsumer()
      throws Exception {
    testConsumer(TEST_TOPIC_1);
    testConsumer(TEST_TOPIC_2);
  }

  private void testConsumer(String topic)
      throws TimeoutException {
    String streamType = "kafka";
    String streamKafkaBrokerList = _kafkaBrokerAddress;
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", topic);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    final StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    int numPartitions = new KafkaStreamMetadataProvider(clientId, streamConfig).fetchPartitionCount(10000);
    for (int partition = 0; partition < numPartitions; partition++) {
      final PartitionLevelConsumer consumer = streamConsumerFactory.createPartitionLevelConsumer(clientId, partition);

      // Test consume a large batch, only 500 records will be returned.
      final MessageBatch batch1 =
          consumer.fetchMessages(new LongMsgOffset(0), new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION), 10000);
      Assert.assertEquals(batch1.getMessageCount(), 500);
      for (int i = 0; i < batch1.getMessageCount(); i++) {
        final byte[] msg = (byte[]) batch1.getMessageAtIndex(i);
        Assert.assertEquals(new String(msg), "sample_msg_" + i);
      }
      // Test second half batch
      final MessageBatch batch2 =
          consumer.fetchMessages(new LongMsgOffset(500), new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION), 10000);
      Assert.assertEquals(batch2.getMessageCount(), 500);
      for (int i = 0; i < batch2.getMessageCount(); i++) {
        final byte[] msg = (byte[]) batch2.getMessageAtIndex(i);
        Assert.assertEquals(new String(msg), "sample_msg_" + (500 + i));
      }
      // Some random range
      final MessageBatch batch3 = consumer.fetchMessages(new LongMsgOffset(10), new LongMsgOffset(35), 10000);
      Assert.assertEquals(batch3.getMessageCount(), 25);
      for (int i = 0; i < batch3.getMessageCount(); i++) {
        final byte[] msg = (byte[]) batch3.getMessageAtIndex(i);
        Assert.assertEquals(new String(msg), "sample_msg_" + (10 + i));
      }
    }
  }

  protected String getKafkaConsumerFactoryName() {
    return KafkaConsumerFactory.class.getName();
  }
}
