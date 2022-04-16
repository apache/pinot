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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.util.TestUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PulsarConsumerTest {

  public static final String TABLE_NAME_WITH_TYPE = "tableName_REALTIME";
  public static final String TEST_TOPIC = "test-topic";
  public static final String TEST_TOPIC_BATCH = "test-topic-batch";
  public static final String MESSAGE_PREFIX = "sample_msg";
  public static final String CLIENT_ID = "clientId";

  public static final int NUM_PARTITION = 1;
  public static final int NUM_RECORDS_PER_PARTITION = 1000;
  public static final int BATCH_SIZE = 10;
  public static final int DEFAULT_TIMEOUT_MS = 10000;
  public static final int DEFAULT_RETRY_COUNT = 3;

  private PulsarClient _pulsarClient;
  private PulsarStandaloneCluster _pulsarStandaloneCluster;
  private HashMap<Integer, MessageId> _partitionToFirstMessageIdMap = new HashMap<>();
  private HashMap<Integer, MessageId> _partitionToFirstMessageIdMapBatch = new HashMap<>();

  @BeforeClass
  public void setUp()
      throws Exception {
    try {
      _pulsarStandaloneCluster = new PulsarStandaloneCluster();

      _pulsarStandaloneCluster.start();

      PulsarAdmin admin =
          PulsarAdmin.builder().serviceHttpUrl("http://localhost:" + _pulsarStandaloneCluster.getAdminPort()).build();

      String bootstrapServer = "pulsar://localhost:" + _pulsarStandaloneCluster.getBrokerPort();

      _pulsarClient = PulsarClient.builder().serviceUrl(bootstrapServer).build();

      admin.topics().createPartitionedTopic(TEST_TOPIC, NUM_PARTITION);
      admin.topics().createPartitionedTopic(TEST_TOPIC_BATCH, NUM_PARTITION);

      publishRecords();
      publishRecordsBatch();
    } catch (Exception e) {
      if (_pulsarStandaloneCluster != null) {
        _pulsarStandaloneCluster.stop();
      }
      throw new RuntimeException("Failed to setUp test environment", e);
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    if (_pulsarStandaloneCluster != null) {
      _pulsarStandaloneCluster.stop();
    }
  }

  public void publishRecords()
      throws Exception {
    for (int p = 0; p < NUM_PARTITION; p++) {
      final int partition = p;
      Producer<String> producer =
          _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC).messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).create();

      for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
        MessageId messageId = producer.send(MESSAGE_PREFIX + "_" + i);
        if (!_partitionToFirstMessageIdMap.containsKey(partition)) {
          _partitionToFirstMessageIdMap.put(partition, messageId);
        }
      }

      producer.flush();
    }
  }

  public void publishRecordsBatch()
      throws Exception {
    for (int p = 0; p < NUM_PARTITION; p++) {
      final int partition = p;
      Producer<String> producer =
          _pulsarClient.newProducer(Schema.STRING).topic(TEST_TOPIC_BATCH).messageRouter(new MessageRouter() {
            @Override
            public int choosePartition(Message<?> msg, TopicMetadata metadata) {
              return partition;
            }
          }).batchingMaxMessages(BATCH_SIZE).batchingMaxPublishDelay(1, TimeUnit.SECONDS).create();

      for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
        CompletableFuture<MessageId> messageIdCompletableFuture = producer.sendAsync(MESSAGE_PREFIX + "_" + i);
        messageIdCompletableFuture.thenAccept(messageId -> {
          if (!_partitionToFirstMessageIdMapBatch.containsKey(partition)) {
            _partitionToFirstMessageIdMapBatch.put(partition, messageId);
          }
        });
      }

      producer.flush();
    }

    // Ensure all message batches are delivered.
    final StreamConsumerFactory streamConsumerFactory =
        StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC_BATCH));
    for (int partition = 0; partition < NUM_PARTITION; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 1, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");
      final PartitionGroupConsumer consumer =
          streamConsumerFactory.createPartitionGroupConsumer(CLIENT_ID, partitionGroupConsumptionStatus);

      TestUtils.waitForCondition(aVoid -> {
        try {
          MessageBatch messageBatch = consumer.fetchMessages(new MessageIdStreamOffset(MessageId.earliest), null,
              DEFAULT_TIMEOUT_MS);
          return messageBatch.getMessageCount() == NUM_RECORDS_PER_PARTITION;
        } catch (TimeoutException e) {
          return false;
        }
      }, 1000L, "Unable to acquire all message batch produced in setup");
    }
  }

  public StreamConfig getStreamConfig(String topicName) {
    String streamType = "pulsar";
    String streamPulsarBrokerList = "pulsar://localhost:" + _pulsarStandaloneCluster.getBrokerPort();
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
    int numPartitions =
        new PulsarStreamMetadataProvider(CLIENT_ID, getStreamConfig(TEST_TOPIC)).fetchPartitionCount(10000);

    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 1, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");

      final PartitionGroupConsumer consumer =
          streamConsumerFactory.createPartitionGroupConsumer(CLIENT_ID, partitionGroupConsumptionStatus);

      consumeMessageBatch(consumer, MessageId.earliest, getMessageIdForPartitionAndIndex(partition, 500), 0, 500);

      consumeMessageBatch(consumer, getMessageIdForPartitionAndIndex(partition, 500), null, 500, 500);

      consumeMessageBatch(consumer, getMessageIdForPartitionAndIndex(partition, 10),
          getMessageIdForPartitionAndIndex(partition, 35), 10, 25);
    }
  }

  @Test
  public void testPartitionLevelConsumerBatchMessages()
      throws Exception {

    final StreamConsumerFactory streamConsumerFactory =
        StreamConsumerFactoryProvider.create(getStreamConfig(TEST_TOPIC_BATCH));
    int numPartitions =
        new PulsarStreamMetadataProvider(CLIENT_ID, getStreamConfig(TEST_TOPIC_BATCH)).fetchPartitionCount(10000);

    for (int partition = 0; partition < numPartitions; partition++) {
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus =
          new PartitionGroupConsumptionStatus(partition, 1, new MessageIdStreamOffset(MessageId.earliest), null,
              "CONSUMING");

      final PartitionGroupConsumer consumer =
          streamConsumerFactory.createPartitionGroupConsumer(CLIENT_ID, partitionGroupConsumptionStatus);
      consumeMessageBatch(consumer, MessageId.earliest, getBatchMessageIdForPartitionAndIndex(partition, 500),
          0, 500);

      consumeMessageBatch(consumer, getBatchMessageIdForPartitionAndIndex(partition, 500), null, 500, 500);

      consumeMessageBatch(consumer, getBatchMessageIdForPartitionAndIndex(partition, 10),
          getBatchMessageIdForPartitionAndIndex(partition, 35), 10, 25);
    }
  }

  private static void consumeMessageBatch(PartitionGroupConsumer consumer, MessageId startMsgId,
      MessageId endMsgId, int expectedMsgOffset, int expectedMsgCount)
      throws TimeoutException {
    TestUtils.waitForCondition(aVoid -> {
      try {
        MessageBatch messageBatch = consumer.fetchMessages(new MessageIdStreamOffset(startMsgId),
            new MessageIdStreamOffset(endMsgId), DEFAULT_TIMEOUT_MS);
        if (messageBatch.getMessageCount() != expectedMsgCount) {
          return false;
        }
        for (int i = 0; i < messageBatch.getMessageCount(); i++) {
          final byte[] msg = (byte[]) messageBatch.getMessageAtIndex(i);
          Assert.assertEquals(new String(msg), "sample_msg_" + (expectedMsgOffset + i));
        }
        return true;
      } catch (Exception e) {
        return false;
      }
    }, 1000L, "Unable to acquire message batch, expected: " + expectedMsgCount);
  }

  private MessageId getMessageIdForPartitionAndIndex(int partitionNum, int index) {
    MessageId startMessageIdRaw = _partitionToFirstMessageIdMap.get(partitionNum);
    MessageIdImpl startMessageId = MessageIdImpl.convertToMessageIdImpl(startMessageIdRaw);
    return DefaultImplementation.newMessageId(startMessageId.getLedgerId(), index, partitionNum);
  }

  private MessageId getBatchMessageIdForPartitionAndIndex(int partitionNum, int index) {
    MessageId startMessageIdRaw = _partitionToFirstMessageIdMapBatch.get(partitionNum);
    BatchMessageIdImpl startMessageId = (BatchMessageIdImpl) MessageIdImpl.convertToMessageIdImpl(startMessageIdRaw);
    return new BatchMessageIdImpl(startMessageId.getLedgerId(), index / BATCH_SIZE, partitionNum, index % BATCH_SIZE,
        startMessageId.getBatchSize(), startMessageId.getAcker());
  }
}
