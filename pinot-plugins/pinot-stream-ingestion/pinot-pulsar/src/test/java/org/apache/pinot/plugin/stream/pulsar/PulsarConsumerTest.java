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
package org.apache.pinot.plugin.stream.pulsar;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class PulsarConsumerTest {
  private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:3.2.2");
  public static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";
  public static final String TEST_TOPIC = "test-topic";
  public static final String TEST_TOPIC_BATCH = "test-topic-batch";
  public static final String MESSAGE_PREFIX = "sample_msg_";
  public static final String CLIENT_ID = "clientId";

  public static final int NUM_PARTITIONS = 2;
  public static final int NUM_RECORDS_PER_PARTITION = 1000;
  public static final int BATCH_SIZE = 10;
  public static final int CONSUMER_FETCH_TIMEOUT_MILLIS = (int) TimeUnit.SECONDS.toMillis(1);

  private final List<List<MessageId>> _partitionToMessageIdMapping = new ArrayList<>(NUM_PARTITIONS);
  private final List<List<MessageId>> _partitionToMessageIdMappingBatch = new ArrayList<>(NUM_PARTITIONS);

  private PulsarContainer _pulsar;

  @BeforeClass
  public void setUp()
      throws Exception {
    _pulsar = new PulsarContainer(PULSAR_IMAGE).withStartupTimeout(Duration.ofMinutes(5));
    _pulsar.start();
    try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(_pulsar.getHttpServiceUrl()).build()) {
      Topics topics = admin.topics();
      topics.createPartitionedTopic(TEST_TOPIC, NUM_PARTITIONS);
      topics.createPartitionedTopic(TEST_TOPIC_BATCH, NUM_PARTITIONS);
    }
    try (PulsarClient client = PulsarClient.builder().serviceUrl(_pulsar.getPulsarBrokerUrl()).build()) {
      publishRecords(client);
      publishRecordsBatch(client);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _pulsar.stop();
  }

  public void publishRecords(PulsarClient client)
      throws Exception {
    for (int p = 0; p < NUM_PARTITIONS; p++) {
      List<MessageId> messageIds = new ArrayList<>(NUM_RECORDS_PER_PARTITION);
      _partitionToMessageIdMapping.add(messageIds);
      int partition = p;
      try (Producer<String> producer = client.newProducer(Schema.STRING).topic(TEST_TOPIC)
          .messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).enableBatching(false).create()) {
        List<Future<MessageId>> futures = new ArrayList<>(NUM_RECORDS_PER_PARTITION);
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          futures.add(producer.sendAsync(MESSAGE_PREFIX + i));
        }
        producer.flush();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          MessageId messageId = futures.get(i).get();
          assertFalse(messageId instanceof BatchMessageIdImpl);
          messageIds.add(messageId);
        }
      }
    }
  }

  public void publishRecordsBatch(PulsarClient client)
      throws Exception {
    for (int p = 0; p < NUM_PARTITIONS; p++) {
      List<MessageId> messageIds = new ArrayList<>(NUM_RECORDS_PER_PARTITION);
      _partitionToMessageIdMappingBatch.add(messageIds);
      int partition = p;
      try (Producer<String> producer = client.newProducer(Schema.STRING).topic(TEST_TOPIC_BATCH)
          .messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).batchingMaxMessages(BATCH_SIZE).batchingMaxPublishDelay(1, TimeUnit.SECONDS).create()) {
        List<Future<MessageId>> futures = new ArrayList<>(NUM_RECORDS_PER_PARTITION);
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          futures.add(producer.sendAsync(MESSAGE_PREFIX + i));
        }
        producer.flush();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          MessageId messageId = futures.get(i).get();
          assertTrue(messageId instanceof BatchMessageIdImpl);
          messageIds.add(messageId);
        }
      }
    }
  }

  public StreamConfig getStreamConfig(String topicName) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "pulsar");
    streamConfigMap.put("stream.pulsar.consumer.type", "simple");
    streamConfigMap.put("stream.pulsar.topic.name", topicName);
    streamConfigMap.put("stream.pulsar.bootstrap.servers", _pulsar.getPulsarBrokerUrl());
    streamConfigMap.put("stream.pulsar.serviceHttpUrl", _pulsar.getHttpServiceUrl());
    streamConfigMap.put("stream.pulsar.consumer.prop.auto.offset.reset", "smallest");
    streamConfigMap.put("stream.pulsar.consumer.factory.class.name", PulsarConsumerFactory.class.getName());
    streamConfigMap.put("stream.pulsar.decoder.class.name", "dummy");
    return new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
  }

  @Test
  public void testPartitionLevelConsumer()
      throws Exception {
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC));
    try (PulsarStreamMetadataProvider metadataProvider = new PulsarStreamMetadataProvider(CLIENT_ID,
        getStreamConfig(TEST_TOPIC))) {
      assertEquals(metadataProvider.fetchPartitionCount(CONSUMER_FETCH_TIMEOUT_MILLIS), NUM_PARTITIONS);
    }
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      List<MessageId> messageIds = _partitionToMessageIdMapping.get(i);
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(i, 0, new MessageIdStreamOffset(MessageId.earliest), null, "CONSUMING");
      try (
          PulsarPartitionLevelConsumer consumer =
              (PulsarPartitionLevelConsumer) streamConsumerFactory.createPartitionGroupConsumer(
              CLIENT_ID, partitionGroupConsumptionStatus)) {
        // Start from earliest
        testConsumer(consumer, 0, messageIds);
        // Start from middle
        testConsumer(consumer, 500, messageIds);
      }
    }
  }

  @Test
  public void testPartitionLevelConsumerBatchMessages()
      throws Exception {
    StreamConsumerFactory streamConsumerFactory =
        StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC_BATCH));
    try (PulsarStreamMetadataProvider metadataProvider = new PulsarStreamMetadataProvider(CLIENT_ID,
        getStreamConfig(TEST_TOPIC_BATCH))) {
      assertEquals(metadataProvider.fetchPartitionCount(CONSUMER_FETCH_TIMEOUT_MILLIS), NUM_PARTITIONS);
    }
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      List<MessageId> messageIds = _partitionToMessageIdMappingBatch.get(i);
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(i, 0, new MessageIdStreamOffset(MessageId.earliest), null, "CONSUMING");
      try (
          PulsarPartitionLevelConsumer consumer =
              (PulsarPartitionLevelConsumer) streamConsumerFactory.createPartitionGroupConsumer(
              CLIENT_ID, partitionGroupConsumptionStatus)) {
        // Start from earliest
        testConsumer(consumer, 0, messageIds);
        // Start from middle
        testConsumer(consumer, 500, messageIds);
      }
    }
  }

  @Test
  public void testGetTopics() 
      throws Exception {
    try (PulsarStreamMetadataProvider metadataProvider = new PulsarStreamMetadataProvider(CLIENT_ID,
        getStreamConfig("NON_EXISTING_TOPIC"))) {
      List<StreamMetadataProvider.TopicMetadata> topics = metadataProvider.getTopics();
      List<String> topicNames = topics.stream()
          .map(StreamMetadataProvider.TopicMetadata::getName)
          .collect(Collectors.toList());
      assertTrue(topicNames.size() == 4);
    }
  }

  private void testConsumer(PulsarPartitionLevelConsumer consumer, int startIndex, List<MessageId> messageIds) {
    MessageId startMessageId = startIndex == 0 ? MessageId.earliest : messageIds.get(startIndex);
    int numMessagesFetched = startIndex;
    while (numMessagesFetched < NUM_RECORDS_PER_PARTITION) {
      PulsarMessageBatch messageBatch =
          consumer.fetchMessages(new MessageIdStreamOffset(startMessageId), CONSUMER_FETCH_TIMEOUT_MILLIS);
      int messageCount = messageBatch.getMessageCount();
      assertFalse(messageBatch.isEndOfPartitionGroup());
      for (int i = 0; i < messageCount; i++) {
        verifyMessage(messageBatch.getStreamMessage(i), numMessagesFetched + i, messageIds);
      }
      numMessagesFetched += messageCount;
      if (numMessagesFetched < NUM_RECORDS_PER_PARTITION) {
        startMessageId = messageIds.get(numMessagesFetched);
      }
    }
    assertEquals(numMessagesFetched, NUM_RECORDS_PER_PARTITION);
  }

  private void verifyMessage(BytesStreamMessage streamMessage, int index, List<MessageId> messageIds) {
    assertEquals(new String(streamMessage.getValue()), MESSAGE_PREFIX + index);
    StreamMessageMetadata messageMetadata = streamMessage.getMetadata();
    assertNotNull(messageMetadata);
    MessageIdStreamOffset offset = (MessageIdStreamOffset) messageMetadata.getOffset();
    assertNotNull(offset);
    MessageIdStreamOffset nextOffset = (MessageIdStreamOffset) messageMetadata.getNextOffset();
    assertNotNull(nextOffset);
    assertEquals(offset.getMessageId(), messageIds.get(index));
    if (index < NUM_RECORDS_PER_PARTITION - 1) {
      assertEquals(nextOffset.getMessageId(), messageIds.get(index + 1));
    }
  }
}
