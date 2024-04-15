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
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;


public class PulsarConsumerTest {
  private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:2.11.4");
  public static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";
  public static final String TEST_TOPIC = "test-topic";
  public static final String TEST_TOPIC_BATCH = "test-topic-batch";
  public static final String MESSAGE_PREFIX = "sample_msg_";
  public static final String CLIENT_ID = "clientId";

  public static final int NUM_PARTITIONS = 2;
  public static final int NUM_RECORDS_PER_PARTITION = 1000;
  public static final int BATCH_SIZE = 10;
  public static final int CONSUMER_FETCH_TIMEOUT_MILLIS = (int) TimeUnit.MINUTES.toMillis(1);

  private final List<List<MessageId>> _partitionToMessageIdMapping = new ArrayList<>(NUM_PARTITIONS);
  private final List<List<MessageId>> _partitionToMessageIdMappingBatch = new ArrayList<>(NUM_PARTITIONS);

  private PulsarContainer _pulsar;
  private PulsarClient _pulsarClient;

  @BeforeClass
  public void setUp()
      throws Exception {
    _pulsar = new PulsarContainer(PULSAR_IMAGE).withStartupTimeout(Duration.ofMinutes(5));
    try {
      _pulsar.start();
      _pulsarClient = PulsarClient.builder().serviceUrl(_pulsar.getPulsarBrokerUrl()).build();

      try (PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(_pulsar.getHttpServiceUrl()).build()) {
        createTopics(admin);
        publishRecords();
        publishRecordsBatch();
        waitForMessagesToPublish(admin, TEST_TOPIC);
        waitForMessagesToPublish(admin, TEST_TOPIC_BATCH);
      }
    } catch (Exception e) {
      _pulsar.stop();
      throw new RuntimeException("Failed to setUp test environment", e);
    }
  }

  private void createTopics(PulsarAdmin admin)
      throws PulsarAdminException {
    InactiveTopicPolicies inactiveTopicPolicies = new InactiveTopicPolicies();
    inactiveTopicPolicies.setDeleteWhileInactive(false);
    admin.namespaces().setInactiveTopicPolicies("public/default", inactiveTopicPolicies);

    admin.topics().createPartitionedTopic(TEST_TOPIC, NUM_PARTITIONS);
    admin.topics().createPartitionedTopic(TEST_TOPIC_BATCH, NUM_PARTITIONS);
  }

  private void waitForMessagesToPublish(PulsarAdmin admin, String topicName)
      throws Exception {
    long endTimeMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
    while (System.currentTimeMillis() < endTimeMs) {
      if (admin.topics().getPartitionedStats(topicName, false).getMsgInCounter()
          == NUM_RECORDS_PER_PARTITION * NUM_PARTITIONS) {
        return;
      }
      Thread.sleep(1000);
    }
    throw new RuntimeException("Failed to publish messages to topic: " + topicName);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    _pulsarClient.close();
    _pulsar.stop();
  }

  public void publishRecords()
      throws Exception {
    for (int p = 0; p < NUM_PARTITIONS; p++) {
      List<MessageId> messageIds = new ArrayList<>(NUM_RECORDS_PER_PARTITION);
      _partitionToMessageIdMapping.add(messageIds);
      int partition = p;
      try (Producer<String> producer = _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC)
          .messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).create()) {
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          messageIds.add(producer.send(MESSAGE_PREFIX + i));
        }
        producer.flush();
      }
    }
  }

  public void publishRecordsBatch()
      throws Exception {
    for (int p = 0; p < NUM_PARTITIONS; p++) {
      List<MessageId> messageIds = new ArrayList<>(NUM_RECORDS_PER_PARTITION);
      _partitionToMessageIdMappingBatch.add(messageIds);
      int partition = p;
      try (Producer<String> producer = _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC_BATCH)
          .messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).batchingMaxMessages(BATCH_SIZE).batchingMaxPublishDelay(1, TimeUnit.SECONDS).create()) {
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          messageIds.add(producer.send(MESSAGE_PREFIX + i));
        }
        producer.flush();
      }
    }
  }

  public StreamConfig getStreamConfig(String topicName) {
    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", "pulsar");
    streamConfigMap.put("stream.pulsar.consumer.type", "simple");
    streamConfigMap.put("stream.pulsar.topic.name", topicName);
    streamConfigMap.put("stream.pulsar.bootstrap.servers", _pulsar.getPulsarBrokerUrl());
    streamConfigMap.put("stream.pulsar.consumer.prop.auto.offset.reset", "smallest");
    streamConfigMap.put("stream.pulsar.consumer.factory.class.name", PulsarConsumerFactory.class.getName());
    streamConfigMap.put("stream.pulsar.decoder.class.name", "dummy");
    return new StreamConfig(TABLE_NAME_WITH_TYPE, streamConfigMap);
  }

  @Test
  public void testPartitionLevelConsumer()
      throws Exception {
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC));
    int numPartitions;
    try (PulsarStreamMetadataProvider metadataProvider = new PulsarStreamMetadataProvider(CLIENT_ID,
        getStreamConfig(TEST_TOPIC))) {
      numPartitions = metadataProvider.fetchPartitionCount(CONSUMER_FETCH_TIMEOUT_MILLIS);
    }

    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 0, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");
      try (
          PulsarPartitionLevelConsumer consumer =
              (PulsarPartitionLevelConsumer) streamConsumerFactory.createPartitionGroupConsumer(
              CLIENT_ID, partitionGroupConsumptionStatus)) {
        PulsarMessageBatch messageBatch =
            consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), CONSUMER_FETCH_TIMEOUT_MILLIS);
        assertEquals(messageBatch.getMessageCount(), 1000);
        assertFalse(messageBatch.isEndOfPartitionGroup());
        for (int i = 0; i < 1000; i++) {
          verifyMessage(messageBatch.getStreamMessage(i), partition, i, false);
        }

        messageBatch =
            consumer.fetchMessages(new MessageIdStreamOffset(_partitionToMessageIdMapping.get(partition).get(500)),
                CONSUMER_FETCH_TIMEOUT_MILLIS);
        assertEquals(messageBatch.getMessageCount(), 500);
        assertFalse(messageBatch.isEndOfPartitionGroup());
        for (int i = 0; i < 500; i++) {
          verifyMessage(messageBatch.getStreamMessage(i), partition, 500 + i, false);
        }
      }
    }
  }

  private void verifyMessage(BytesStreamMessage streamMessage, int partition, int index, boolean batch) {
    assertEquals(new String(streamMessage.getValue()), MESSAGE_PREFIX + index);
    StreamMessageMetadata messageMetadata = streamMessage.getMetadata();
    assertNotNull(messageMetadata);
    MessageIdStreamOffset offset = (MessageIdStreamOffset) messageMetadata.getOffset();
    assertNotNull(offset);
    MessageIdStreamOffset nextOffset = (MessageIdStreamOffset) messageMetadata.getNextOffset();
    assertNotNull(nextOffset);
    List<MessageId> messageIds =
        batch ? _partitionToMessageIdMappingBatch.get(partition) : _partitionToMessageIdMapping.get(partition);
    assertEquals(offset.getMessageId(), messageIds.get(index));
    if (index < NUM_RECORDS_PER_PARTITION - 1) {
      assertEquals(nextOffset.getMessageId(), messageIds.get(index + 1));
    }
  }

  @Test
  public void testPartitionLevelConsumerBatchMessages()
      throws Exception {
    StreamConsumerFactory streamConsumerFactory =
        StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC_BATCH));
    int numPartitions;
    try (PulsarStreamMetadataProvider metadataProvider = new PulsarStreamMetadataProvider(CLIENT_ID,
        getStreamConfig(TEST_TOPIC_BATCH))) {
      numPartitions = metadataProvider.fetchPartitionCount(CONSUMER_FETCH_TIMEOUT_MILLIS);
    }

    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 0, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");
      try (
          PulsarPartitionLevelConsumer consumer =
              (PulsarPartitionLevelConsumer) streamConsumerFactory.createPartitionGroupConsumer(
              CLIENT_ID, partitionGroupConsumptionStatus)) {
        PulsarMessageBatch messageBatch =
            consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), CONSUMER_FETCH_TIMEOUT_MILLIS);
        assertEquals(messageBatch.getMessageCount(), 1000);
        assertFalse(messageBatch.isEndOfPartitionGroup());
        for (int i = 0; i < 1000; i++) {
          verifyMessage(messageBatch.getStreamMessage(i), partition, i, true);
        }

        messageBatch =
            consumer.fetchMessages(new MessageIdStreamOffset(_partitionToMessageIdMappingBatch.get(partition).get(500)),
                CONSUMER_FETCH_TIMEOUT_MILLIS);
        assertEquals(messageBatch.getMessageCount(), 500);
        assertFalse(messageBatch.isEndOfPartitionGroup());
        for (int i = 0; i < 500; i++) {
          verifyMessage(messageBatch.getStreamMessage(i), partition, 500 + i, true);
        }
      }
    }
  }
}
