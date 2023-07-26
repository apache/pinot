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

import com.google.common.base.Function;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PulsarConsumerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConsumerTest.class);

  private static final DockerImageName PULSAR_IMAGE = DockerImageName.parse("apachepulsar/pulsar:2.7.2");
  public static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";
  public static final String TEST_TOPIC = "test-topic";
  public static final String TEST_TOPIC_BATCH = "test-topic-batch";
  public static final String MESSAGE_PREFIX = "sample_msg";
  public static final String CLIENT_ID = "clientId";

  public static final int NUM_PARTITION = 1;
  public static final int NUM_RECORDS_PER_PARTITION = 1000;
  public static final int BATCH_SIZE = 10;
  public static final int CONSUMER_FETCH_TIMEOUT_MILLIS = (int) Duration.ofMinutes(5).toMillis();

  private PulsarClient _pulsarClient;
  private PulsarContainer _pulsar = null;
  private HashMap<Integer, MessageId> _partitionToFirstMessageIdMap = new HashMap<>();
  private HashMap<Integer, MessageId> _partitionToFirstMessageIdMapBatch = new HashMap<>();
  private ConcurrentHashMap<Integer, List<BatchMessageIdImpl>> _partitionToMessageIdMapping = new ConcurrentHashMap<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    try {
      _pulsar = new PulsarContainer(PULSAR_IMAGE).withStartupTimeout(Duration.ofMinutes(5));
      _pulsar.start();

      // Waiting for namespace to be created.
      // There should be a better approach.
      Thread.sleep(20 * 1000L);

      PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(_pulsar.getHttpServiceUrl()).build();

      String bootstrapServer = _pulsar.getPulsarBrokerUrl();

      _pulsarClient = PulsarClient.builder().serviceUrl(bootstrapServer).build();

      createTopics(admin);

      publishRecords();
      publishRecordsBatch();

      waitForMessagesToPublish(admin, TEST_TOPIC);
      waitForMessagesToPublish(admin, TEST_TOPIC_BATCH);

      admin.close();
    } catch (Exception e) {
      if (_pulsar != null) {
        _pulsar.stop();
        _pulsar = null;
      }
      throw new RuntimeException("Failed to setUp test environment", e);
    }
  }

  private void createTopics(PulsarAdmin admin)
      throws PulsarAdminException {
    InactiveTopicPolicies inactiveTopicPolicies = new InactiveTopicPolicies();
    inactiveTopicPolicies.setDeleteWhileInactive(false);
    admin.namespaces().setInactiveTopicPolicies("public/default", inactiveTopicPolicies);

    admin.topics().createPartitionedTopic(TEST_TOPIC, NUM_PARTITION);
    admin.topics().createPartitionedTopic(TEST_TOPIC_BATCH, NUM_PARTITION);
  }

  private void waitForMessagesToPublish(PulsarAdmin admin, String topicName) {
    waitForCondition(new Function<Void, Boolean>() {
      @Nullable
      @Override
      public Boolean apply(@Nullable Void aVoid) {
        try {
          return getNumberOfEntries(admin, topicName) == NUM_RECORDS_PER_PARTITION * NUM_PARTITION;
        } catch (Exception e) {
          LOGGER.warn("Could not fetch number of messages in pulsar topic " + topicName, e);
          return null;
        }
      }
    }, 2000L, 60 * 1000L, "Failed to produce " + NUM_RECORDS_PER_PARTITION * NUM_PARTITION + " messages", true);
  }

  private long getNumberOfEntries(PulsarAdmin admin, String topicName) {
    try {
      return admin.topics().getPartitionedStats(topicName, false).getMsgInCounter();
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.warn("Could not fetch number of rows in pulsar topic " + topicName, e);
    }
    return -1;
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_pulsar != null) {
      _pulsar.stop();
      _pulsarClient.close();
      _partitionToMessageIdMapping.clear();
      _pulsar = null;
    }
  }

  public void publishRecords()
      throws Exception {
    for (int p = 0; p < NUM_PARTITION; p++) {
      final int partition = p;
      try (Producer<String> producer = _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC)
          .messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).create()) {
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          MessageId messageId = producer.send(MESSAGE_PREFIX + "_" + i);
          if (!_partitionToFirstMessageIdMap.containsKey(partition)) {
            _partitionToFirstMessageIdMap.put(partition, messageId);
          }
        }
        producer.flush();
      }
      waitForCondition(input -> validatePartitionMessageCount(partition, NUM_RECORDS_PER_PARTITION, TEST_TOPIC),
          1 * 1000L, 5 * 60 * 1000L,
          "Failed to consume " + NUM_RECORDS_PER_PARTITION + " messages from partition " + partition, true);
    }
  }

  public void publishRecordsBatch()
      throws Exception {
    for (int p = 0; p < NUM_PARTITION; p++) {
      final int partition = p;
      try (Producer<String> producer = _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC_BATCH)
          .messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).batchingMaxMessages(BATCH_SIZE).batchingMaxPublishDelay(1, TimeUnit.SECONDS).create()) {
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
          CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(MESSAGE_PREFIX + "_" + i);
          messageIdCompletableFuture.thenAccept(messageId -> {

            _partitionToMessageIdMapping.compute(partition, (partitionId, messageIds) -> {
              if (messageIds == null) {
                List<BatchMessageIdImpl> messageIdList = new ArrayList<>();
                messageIdList.add((BatchMessageIdImpl) messageId);
                if (!_partitionToFirstMessageIdMapBatch.containsKey(partition)) {
                  _partitionToFirstMessageIdMapBatch.put(partition, messageId);
                }
                return messageIdList;
              } else {
                messageIds.add((BatchMessageIdImpl) messageId);
                return messageIds;
              }
            });
          });
        }
        producer.flush();
      }
      waitForCondition(input -> validatePartitionMessageCount(partition, NUM_RECORDS_PER_PARTITION, TEST_TOPIC_BATCH),
          1 * 1000L, 5 * 60 * 1000L,
          "Failed to consume " + NUM_RECORDS_PER_PARTITION + " messages from partition " + partition, true);
    }
  }

  private boolean validatePartitionMessageCount(int partition, int expectedMsgCount, String topicName) {
    final PartitionGroupConsumer consumer = StreamConsumerFactoryProvider.create(getStreamConfig(topicName))
        .createPartitionGroupConsumer(CLIENT_ID,
            new PartitionGroupConsumptionStatus(partition, 1, new MessageIdStreamOffset(MessageId.earliest), null,
                "CONSUMING"));
    try {
      final MessageBatch messageBatch =
          consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), null, CONSUMER_FETCH_TIMEOUT_MILLIS);
      LOGGER.info("Partition: " + partition + ", Consumed messageBatch count = " + messageBatch.getMessageCount());
      return messageBatch.getMessageCount() == expectedMsgCount;
    } catch (TimeoutException e) {
      return false;
    }
  }

  public StreamConfig getStreamConfig(String topicName) {
    String streamType = "pulsar";
    String streamPulsarBrokerList = _pulsar.getPulsarBrokerUrl();
    String streamPulsarConsumerType = "simple";
    String tableNameWithType = TABLE_NAME_WITH_TYPE;

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.pulsar.consumer.type", streamPulsarConsumerType);
    streamConfigMap.put("stream.pulsar.topic.name", topicName);
    streamConfigMap.put("stream.pulsar.bootstrap.servers", streamPulsarBrokerList);
    streamConfigMap.put("stream.pulsar.consumer.prop.auto.offset.reset", "smallest");
    streamConfigMap.put("stream.pulsar.consumer.factory.class.name", getPulsarConsumerFactoryName());
    streamConfigMap.put(
        StreamConfigProperties.constructStreamProperty("pulsar", StreamConfigProperties.STREAM_FETCH_TIMEOUT_MILLIS),
        "1000");
    streamConfigMap.put("stream.pulsar.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    return streamConfig;
  }

  protected String getPulsarConsumerFactoryName() {
    return PulsarConsumerFactory.class.getName();
  }

  @Test
  public void testPartitionLevelConsumer()
      throws Exception {

    final StreamConsumerFactory streamConsumerFactory =
        StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC));
    int numPartitions = new PulsarStreamMetadataProvider(CLIENT_ID, getStreamConfig(TEST_TOPIC)).fetchPartitionCount(
        CONSUMER_FETCH_TIMEOUT_MILLIS);

    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 1, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");

      int totalMessagesReceived = 0;

      final PulsarPartitionLevelConsumer consumer =
              (PulsarPartitionLevelConsumer) streamConsumerFactory
                      .createPartitionGroupConsumer(CLIENT_ID, partitionGroupConsumptionStatus);
      final PulsarMessageBatch messageBatch1 = consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest),
          new MessageIdStreamOffset(getMessageIdForPartitionAndIndex(partition, 500)), CONSUMER_FETCH_TIMEOUT_MILLIS);
      Assert.assertEquals(messageBatch1.getMessageCount(), 500);
      for (int i = 0; i < messageBatch1.getMessageCount(); i++) {
        final byte[] msg = messageBatch1.getMessageAtIndex(i).getValue();
        Assert.assertEquals(new String(msg), "sample_msg_" + i);
        totalMessagesReceived++;
      }

      final PulsarMessageBatch messageBatch2 =
          consumer.fetchMessages(new MessageIdStreamOffset(getMessageIdForPartitionAndIndex(partition, 500)), null,
              CONSUMER_FETCH_TIMEOUT_MILLIS);
      Assert.assertEquals(messageBatch2.getMessageCount(), 500);
      for (int i = 0; i < messageBatch2.getMessageCount(); i++) {
        final byte[] msg = messageBatch2.getMessageAtIndex(i).getValue();
        Assert.assertEquals(new String(msg), "sample_msg_" + (500 + i));
        totalMessagesReceived++;
      }

      final PulsarMessageBatch messageBatch3 =
          consumer.fetchMessages(new MessageIdStreamOffset(getMessageIdForPartitionAndIndex(partition, 10)),
              new MessageIdStreamOffset(getMessageIdForPartitionAndIndex(partition, 35)),
              CONSUMER_FETCH_TIMEOUT_MILLIS);
      Assert.assertEquals(messageBatch3.getMessageCount(), 25);
      for (int i = 0; i < messageBatch3.getMessageCount(); i++) {
        final byte[] msg = messageBatch3.getMessageAtIndex(i).getValue();
        Assert.assertEquals(new String(msg), "sample_msg_" + (10 + i));
      }

      Assert.assertEquals(totalMessagesReceived, NUM_RECORDS_PER_PARTITION);
    }
  }

  @Test
  public void testPartitionLevelConsumerBatchMessages()
      throws Exception {

    final StreamConsumerFactory streamConsumerFactory =
        StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC_BATCH));
    int numPartitions =
        new PulsarStreamMetadataProvider(CLIENT_ID, getStreamConfig(TEST_TOPIC_BATCH)).fetchPartitionCount(
            CONSUMER_FETCH_TIMEOUT_MILLIS);

    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 1, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");

      int totalMessagesReceived = 0;

      final PulsarPartitionLevelConsumer consumer =
              (PulsarPartitionLevelConsumer) streamConsumerFactory.createPartitionGroupConsumer(CLIENT_ID,
                  partitionGroupConsumptionStatus);
      //TODO: This test failed, check it out.
      final PulsarMessageBatch messageBatch1 = consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest),
          new MessageIdStreamOffset(getBatchMessageIdForPartitionAndIndex(partition, 500)),
          CONSUMER_FETCH_TIMEOUT_MILLIS);
      Assert.assertEquals(messageBatch1.getMessageCount(), 500);
      for (int i = 0; i < messageBatch1.getMessageCount(); i++) {
        final byte[] msg = messageBatch1.getMessageAtIndex(i).getValue();
        Assert.assertEquals(new String(msg), "sample_msg_" + i);
        totalMessagesReceived++;
      }

      final PulsarMessageBatch messageBatch2 =
          consumer.fetchMessages(new MessageIdStreamOffset(getBatchMessageIdForPartitionAndIndex(partition, 500)), null,
              CONSUMER_FETCH_TIMEOUT_MILLIS);
      Assert.assertEquals(messageBatch2.getMessageCount(), 500);
      for (int i = 0; i < messageBatch2.getMessageCount(); i++) {
        final byte[] msg = messageBatch2.getMessageAtIndex(i).getValue();
        Assert.assertEquals(new String(msg), "sample_msg_" + (500 + i));
        totalMessagesReceived++;
      }

      final PulsarMessageBatch messageBatch3 =
          consumer.fetchMessages(new MessageIdStreamOffset(getBatchMessageIdForPartitionAndIndex(partition, 10)),
              new MessageIdStreamOffset(getBatchMessageIdForPartitionAndIndex(partition, 35)),
              CONSUMER_FETCH_TIMEOUT_MILLIS);
      Assert.assertEquals(messageBatch3.getMessageCount(), 25);
      for (int i = 0; i < messageBatch3.getMessageCount(); i++) {
        final byte[] msg = messageBatch3.getMessageAtIndex(i).getValue();
        Assert.assertEquals(new String(msg), "sample_msg_" + (10 + i));
      }

      Assert.assertEquals(totalMessagesReceived, NUM_RECORDS_PER_PARTITION);
    }
  }

  private MessageId getMessageIdForPartitionAndIndex(int partitionNum, int index) {
    MessageId startMessageIdRaw = _partitionToFirstMessageIdMap.get(partitionNum);
    MessageIdImpl startMessageId = MessageIdImpl.convertToMessageIdImpl(startMessageIdRaw);
    return DefaultImplementation.getDefaultImplementation()
        .newMessageId(startMessageId.getLedgerId(), index, partitionNum);
  }

  private MessageId getBatchMessageIdForPartitionAndIndex(int partitionNum, int index) {
    return _partitionToMessageIdMapping.get(partitionNum).get(index);
  }

  private void waitForCondition(Function<Void, Boolean> condition, long checkIntervalMs, long timeoutMs,
      @Nullable String errorMessage, boolean raiseError) {
    long endTime = System.currentTimeMillis() + timeoutMs;
    String errorMessageSuffix = errorMessage != null ? ", error message: " + errorMessage : "";
    while (System.currentTimeMillis() < endTime) {
      try {
        if (Boolean.TRUE.equals(condition.apply(null))) {
          return;
        }
        Thread.sleep(checkIntervalMs);
      } catch (Exception e) {
        Assert.fail("Caught exception while checking the condition" + errorMessageSuffix, e);
      }
    }
    if (raiseError) {
      Assert.fail("Failed to meet condition in " + timeoutMs + "ms" + errorMessageSuffix);
    }
  }
}
