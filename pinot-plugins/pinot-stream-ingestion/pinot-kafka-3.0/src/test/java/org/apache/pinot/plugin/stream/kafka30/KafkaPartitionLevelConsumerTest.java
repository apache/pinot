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
package org.apache.pinot.plugin.stream.kafka30;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.plugin.stream.kafka30.utils.MiniKafkaCluster;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMessage;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Tests for the KafkaPartitionLevelConsumer.
 */
public class KafkaPartitionLevelConsumerTest {
  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC_1 = "foo";
  private static final String TEST_TOPIC_2 = "bar";
  private static final String TEST_TOPIC_3 = "expired";
  private static final int NUM_MSG_PRODUCED_PER_PARTITION = 1000;
  private static final long TIMESTAMP = Instant.now().toEpochMilli();

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
    _kafkaCluster.createTopic(TEST_TOPIC_3, 1, 1);
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
    produceMsgToKafka();
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
    _kafkaCluster.deleteRecordsBeforeOffset(TEST_TOPIC_3, 0, 200);
  }

  private void produceMsgToKafka() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaBrokerAddress);
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < NUM_MSG_PRODUCED_PER_PARTITION; i++) {
        producer.send(new ProducerRecord<>(TEST_TOPIC_1, 0, TIMESTAMP + i, null, "sample_msg_" + i));
        // TEST_TOPIC_2 has 2 partitions
        producer.send(new ProducerRecord<>(TEST_TOPIC_2, 0, TIMESTAMP + i, null, "sample_msg_" + i));
        producer.send(new ProducerRecord<>(TEST_TOPIC_2, 1, TIMESTAMP + i, null, "sample_msg_" + i));
        producer.send(new ProducerRecord<>(TEST_TOPIC_3, "sample_msg_" + i));
      }
      producer.flush();
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    try {
      _kafkaCluster.deleteTopic(TEST_TOPIC_1);
      _kafkaCluster.deleteTopic(TEST_TOPIC_2);
      _kafkaCluster.deleteTopic(TEST_TOPIC_3);
    } finally {
      _kafkaCluster.close();
    }
  }

  @Test
  public void testBuildConsumer() {
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
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), 10000);

    assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaBufferSize());
    assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaSocketTimeout());

    // test parsing values
    assertEquals(10000, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaFetcherSizeBytes());
    assertEquals(20000, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaFetcherMinBytes());

    // test user defined values
    streamConfigMap.put("stream.kafka.buffer.size", "100");
    streamConfigMap.put("stream.kafka.socket.timeout", "1000");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
    kafkaSimpleStreamConsumer = new KafkaPartitionLevelConsumer(clientId, streamConfig, 0);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), 10000);
    assertEquals(100, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaBufferSize());
    assertEquals(1000, kafkaSimpleStreamConsumer.getKafkaPartitionLevelStreamConfig().getKafkaSocketTimeout());
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
    assertEquals(streamMetadataProvider.fetchPartitionCount(10000L), 1);

    streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC_2);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    streamMetadataProvider = new KafkaStreamMetadataProvider(clientId, streamConfig);
    assertEquals(streamMetadataProvider.fetchPartitionCount(10000L), 2);
  }

  @Test
  public void testFetchMessages() {
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
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), 10000);
  }

  @Test
  public void testFetchOffsets() {
    testFetchOffsets(TEST_TOPIC_1);
    testFetchOffsets(TEST_TOPIC_2);
  }

  private void testFetchOffsets(String topic) {
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
      assertEquals(new LongMsgOffset(0).compareTo(kafkaStreamMetadataProvider.fetchStreamPartitionOffset(
          new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000)), 0);
      assertEquals(new LongMsgOffset(0).compareTo(kafkaStreamMetadataProvider.fetchStreamPartitionOffset(
          new OffsetCriteria.OffsetCriteriaBuilder().withOffsetAsPeriod("2d"), 10000)), 0);
      assertEquals(new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION).compareTo(
          kafkaStreamMetadataProvider.fetchStreamPartitionOffset(
              new OffsetCriteria.OffsetCriteriaBuilder().withOffsetAsTimestamp(Instant.now().toString()), 10000)), 0);
      assertEquals(new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION).compareTo(
          kafkaStreamMetadataProvider.fetchStreamPartitionOffset(
              new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest(), 10000)), 0);
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

    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    int numPartitions = new KafkaStreamMetadataProvider(clientId, streamConfig).fetchPartitionCount(10000);
    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumer consumer = streamConsumerFactory.createPartitionGroupConsumer(clientId,
          new PartitionGroupConsumptionStatus(partition, 0, new LongMsgOffset(0),
              new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION), "CONSUMING"));

      // Test consume a large batch, only 500 records will be returned.
      MessageBatch messageBatch = consumer.fetchMessages(new LongMsgOffset(0), 10000);
      assertEquals(messageBatch.getMessageCount(), 500);
      assertEquals(messageBatch.getUnfilteredMessageCount(), 500);
      for (int i = 0; i < 500; i++) {
        StreamMessage streamMessage = messageBatch.getStreamMessage(i);
        assertEquals(new String((byte[]) streamMessage.getValue()), "sample_msg_" + i);
        StreamMessageMetadata metadata = streamMessage.getMetadata();
        assertNotNull(metadata);
        assertEquals(metadata.getRecordIngestionTimeMs(), TIMESTAMP + i);
        StreamPartitionMsgOffset offset = metadata.getOffset();
        assertTrue(offset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) offset).getOffset(), i);
        StreamPartitionMsgOffset nextOffset = metadata.getNextOffset();
        assertTrue(nextOffset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) nextOffset).getOffset(), i + 1);
      }
      assertEquals(messageBatch.getOffsetOfNextBatch().toString(), "500");
      assertEquals(messageBatch.getFirstMessageOffset().toString(), "0");
      assertEquals(messageBatch.getLastMessageMetadata().getOffset().toString(), "499");
      assertEquals(messageBatch.getLastMessageMetadata().getNextOffset().toString(), "500");

      // Test second half batch
      messageBatch = consumer.fetchMessages(new LongMsgOffset(500), 10000);
      assertEquals(messageBatch.getMessageCount(), 500);
      assertEquals(messageBatch.getUnfilteredMessageCount(), 500);
      for (int i = 0; i < 500; i++) {
        StreamMessage streamMessage = messageBatch.getStreamMessage(i);
        assertEquals(new String((byte[]) streamMessage.getValue()), "sample_msg_" + (500 + i));
        StreamMessageMetadata metadata = streamMessage.getMetadata();
        assertNotNull(metadata);
        assertEquals(metadata.getRecordIngestionTimeMs(), TIMESTAMP + 500 + i);
        StreamPartitionMsgOffset offset = metadata.getOffset();
        assertTrue(offset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) offset).getOffset(), 500 + i);
        StreamPartitionMsgOffset nextOffset = metadata.getNextOffset();
        assertTrue(nextOffset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) nextOffset).getOffset(), 501 + i);
      }
      assertEquals(messageBatch.getOffsetOfNextBatch().toString(), "1000");
      assertEquals(messageBatch.getFirstMessageOffset().toString(), "500");
      assertEquals(messageBatch.getLastMessageMetadata().getOffset().toString(), "999");
      assertEquals(messageBatch.getLastMessageMetadata().getNextOffset().toString(), "1000");

      // Some random range
      messageBatch = consumer.fetchMessages(new LongMsgOffset(10), 10000);
      assertEquals(messageBatch.getMessageCount(), 500);
      assertEquals(messageBatch.getUnfilteredMessageCount(), 500);
      for (int i = 0; i < 500; i++) {
        StreamMessage streamMessage = messageBatch.getStreamMessage(i);
        assertEquals(new String((byte[]) streamMessage.getValue()), "sample_msg_" + (10 + i));
        StreamMessageMetadata metadata = streamMessage.getMetadata();
        assertNotNull(metadata);
        assertEquals(metadata.getRecordIngestionTimeMs(), TIMESTAMP + 10 + i);
        StreamPartitionMsgOffset offset = metadata.getOffset();
        assertTrue(offset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) offset).getOffset(), 10 + i);
        StreamPartitionMsgOffset nextOffset = metadata.getNextOffset();
        assertTrue(nextOffset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) nextOffset).getOffset(), 11 + i);
      }
      assertEquals(messageBatch.getOffsetOfNextBatch().toString(), "510");
      assertEquals(messageBatch.getFirstMessageOffset().toString(), "10");
      assertEquals(messageBatch.getLastMessageMetadata().getOffset().toString(), "509");
      assertEquals(messageBatch.getLastMessageMetadata().getNextOffset().toString(), "510");

      // Some random range
      messageBatch = consumer.fetchMessages(new LongMsgOffset(610), 10000);
      assertEquals(messageBatch.getMessageCount(), 390);
      assertEquals(messageBatch.getUnfilteredMessageCount(), 390);
      for (int i = 0; i < 390; i++) {
        StreamMessage streamMessage = messageBatch.getStreamMessage(i);
        assertEquals(new String((byte[]) streamMessage.getValue()), "sample_msg_" + (610 + i));
        StreamMessageMetadata metadata = streamMessage.getMetadata();
        assertNotNull(metadata);
        assertEquals(metadata.getRecordIngestionTimeMs(), TIMESTAMP + 610 + i);
        StreamPartitionMsgOffset offset = metadata.getOffset();
        assertTrue(offset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) offset).getOffset(), 610 + i);
        StreamPartitionMsgOffset nextOffset = metadata.getNextOffset();
        assertTrue(nextOffset instanceof LongMsgOffset);
        assertEquals(((LongMsgOffset) nextOffset).getOffset(), 611 + i);
      }
      assertEquals(messageBatch.getOffsetOfNextBatch().toString(), "1000");
      assertEquals(messageBatch.getFirstMessageOffset().toString(), "610");
      assertEquals(messageBatch.getLastMessageMetadata().getOffset().toString(), "999");
      assertEquals(messageBatch.getLastMessageMetadata().getNextOffset().toString(), "1000");
    }
  }

  protected String getKafkaConsumerFactoryName() {
    return KafkaConsumerFactory.class.getName();
  }

  @Test
  public void testOffsetsExpired()
      throws TimeoutException {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "kafka");
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC_3);
    streamConfigMap.put("stream.kafka.broker.list", _kafkaBrokerAddress);
    streamConfigMap.put("stream.kafka.consumer.type", "lowlevel");
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfigMap.put("auto.offset.reset", "earliest");
    StreamConfig streamConfig = new StreamConfig("tableName_REALTIME", streamConfigMap);

    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    PartitionGroupConsumer consumer = streamConsumerFactory.createPartitionGroupConsumer("clientId",
        new PartitionGroupConsumptionStatus(0, 0, new LongMsgOffset(0),
            new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION), "CONSUMING"));

    // Start offset has expired. Automatically reset to earliest available and fetch whatever available
    MessageBatch messageBatch = consumer.fetchMessages(new LongMsgOffset(0), 10000);
    assertEquals(messageBatch.getMessageCount(), 500);
    assertEquals(messageBatch.getUnfilteredMessageCount(), 500);
    for (int i = 0; i < 500; i++) {
      assertEquals(new String((byte[]) messageBatch.getStreamMessage(i).getValue()), "sample_msg_" + (200 + i));
    }
    assertEquals(messageBatch.getOffsetOfNextBatch().toString(), "700");
  }

  @Test
  public void testListTopics() {
    String streamType = "kafka";
    String streamKafkaBrokerList = _kafkaBrokerAddress;
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", "NON_EXISTING_TOPIC");
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", getKafkaConsumerFactoryName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    KafkaStreamMetadataProvider streamMetadataProvider = new KafkaStreamMetadataProvider(clientId, streamConfig);
    List<StreamMetadataProvider.TopicMetadata> topics =
        streamMetadataProvider.listTopics(Duration.ofSeconds(60));
    Set<String> topicNames = topics.stream().map(StreamMetadataProvider.TopicMetadata::getName).collect(Collectors.toSet());
    assertEquals(topicNames, Set.of(TEST_TOPIC_1, TEST_TOPIC_2, TEST_TOPIC_3));
  }
}
