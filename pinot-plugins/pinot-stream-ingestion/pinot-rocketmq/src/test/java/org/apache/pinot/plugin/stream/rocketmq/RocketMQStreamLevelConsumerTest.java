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
package org.apache.pinot.plugin.stream.rocketmq;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.pinot.plugin.stream.rocketmq.utils.MiniRocketMQCluster;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamConsumerFactoryProvider;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RocketMQStreamLevelConsumerTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQStreamLevelConsumerTest.class);

  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC_1 = "foo";
  private static final String TEST_TOPIC_2 = "bar";
  private static final int NUM_MSG_PRODUCED_PER_PARTITION = 1000;

  private MiniRocketMQCluster _rocketmqCluster;
  private String _namesrvAddress;

  @BeforeClass
  public void setUp()
      throws Exception {
    _rocketmqCluster = new MiniRocketMQCluster("default_cluster", "default_broker");
    _rocketmqCluster.start();
    _namesrvAddress = _rocketmqCluster.getNamesrvAddress();
    LOGGER.info("nameserver: {}", _namesrvAddress);
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
    _rocketmqCluster.initTopic(TEST_TOPIC_1, 1);
    _rocketmqCluster.initTopic(TEST_TOPIC_2, 2);
    produceMsgToRocketMQ();
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
  }

  private void produceMsgToRocketMQ() {
    DefaultMQProducer producer = new DefaultMQProducer("TEST_PRODUCER");
    producer.setNamesrvAddr(_namesrvAddress);
    try {
      producer.start();

      MessageQueueSelector queueSelector = (mqs, msg, arg) -> {
        return mqs.stream().filter(q -> arg.equals(q.getQueueId())).collect(Collectors.toList()).get(0);
      };

      for (int i = 0; i < NUM_MSG_PRODUCED_PER_PARTITION; i++) {
        producer.send(new Message(TEST_TOPIC_1, ("sample_msg_" + i).getBytes(StandardCharsets.UTF_8)));
        // TEST_TOPIC_2 has 2 partitions
        producer.send(new Message(TEST_TOPIC_2, ("sample_msg_0_" + i).getBytes(StandardCharsets.UTF_8)), queueSelector,
            0);
        producer.send(new Message(TEST_TOPIC_2, ("sample_msg_1_" + i).getBytes(StandardCharsets.UTF_8)), queueSelector,
            1);
      }
    } catch (Throwable e) {
      Assert.fail("failed to produce messages", e);
    } finally {
      producer.shutdown();
    }
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    try {
      _rocketmqCluster.close();
    } catch (Exception e) {
      LOGGER.warn("Exception on close RocketMQCluster", e);
    }
  }

  @Test
  public void testBuildRocketMQConfig()
      throws Exception {
    String streamType = "rocketmq";
    String streamRocketMQTopicName = "theTopic";
    String streamRocketMQNameServerAddress = _namesrvAddress;
    String streamRocketMQConsumerType = "simple";
    String groupId = "groupId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.rocketmq.topic.name", streamRocketMQTopicName);
    streamConfigMap.put("stream.rocketmq.nameserver.list", streamRocketMQNameServerAddress);
    streamConfigMap.put("stream.rocketmq.consumer.type", streamRocketMQConsumerType);
    streamConfigMap.put("stream.rocketmq.consumer.factory.class.name", getRocketMQConsumerFactoryName());
    streamConfigMap.put("stream.rocketmq.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
    RocketMQConfig rocketmqStreamLevelStreamConfig = new RocketMQConfig(streamConfig, groupId);

    // test parsing values
    Assert.assertEquals(streamRocketMQNameServerAddress, rocketmqStreamLevelStreamConfig.getNameServer());
    Assert.assertEquals(streamRocketMQTopicName, rocketmqStreamLevelStreamConfig.getRocketMQTopicName());

    // test default value
    Assert.assertEquals(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET,
        rocketmqStreamLevelStreamConfig.getConsumerFromWhere());
    Assert.assertNull(rocketmqStreamLevelStreamConfig.getConsumeTimestamp());

    // test user defined values
    streamConfigMap.put("stream.rocketmq.consumer.prop.auto.offset.reset", "1645527240000");
    streamConfigMap.put("stream.rocketmq.consumer.prop.namespace", "dev");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
    rocketmqStreamLevelStreamConfig = new RocketMQConfig(streamConfig, groupId);
    Assert.assertEquals(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP,
        rocketmqStreamLevelStreamConfig.getConsumerFromWhere());
    Assert.assertEquals(rocketmqStreamLevelStreamConfig.getConsumeTimestamp(), "1645527240000");
    Assert.assertEquals(rocketmqStreamLevelStreamConfig.getConsumerNamespace(), "dev");
  }

  @Test
  public void testGetPartitionCount() {
    String streamType = "rocketmq";
    String streamRocketMQNameServerAddress = _namesrvAddress;
    String streamRocketMQConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.rocketmq.topic.name", TEST_TOPIC_1);
    streamConfigMap.put("stream.rocketmq.nameserver.list", streamRocketMQNameServerAddress);
    streamConfigMap.put("stream.rocketmq.consumer.type", streamRocketMQConsumerType);
    streamConfigMap.put("stream.rocketmq.consumer.factory.class.name", getRocketMQConsumerFactoryName());
    streamConfigMap.put("stream.rocketmq.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    RocketMQStreamMetadataProvider streamMetadataProvider = new RocketMQStreamMetadataProvider(clientId, streamConfig);
    Assert.assertEquals(streamMetadataProvider.fetchPartitionCount(1000L), 1);

    streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.rocketmq.topic.name", TEST_TOPIC_2);
    streamConfigMap.put("stream.rocketmq.nameserver.list", streamRocketMQNameServerAddress);
    streamConfigMap.put("stream.rocketmq.consumer.type", streamRocketMQConsumerType);
    streamConfigMap.put("stream.rocketmq.consumer.factory.class.name", getRocketMQConsumerFactoryName());
    streamConfigMap.put("stream.rocketmq.decoder.class.name", "decoderClass");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    streamMetadataProvider = new RocketMQStreamMetadataProvider(clientId, streamConfig);
    Assert.assertEquals(streamMetadataProvider.fetchPartitionCount(1000L), 2);
  }

  @Test
  public void testFetchOffsets()
      throws Exception {
    testFetchOffsets(TEST_TOPIC_1);
    testFetchOffsets(TEST_TOPIC_2);
  }

  private void testFetchOffsets(String topic)
      throws Exception {
    String streamType = "rocketmq";
    String streamRocketMQNameServerAddress = _namesrvAddress;
    String streamRocketMQConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.rocketmq.topic.name", topic);
    streamConfigMap.put("stream.rocketmq.nameserver.list", streamRocketMQNameServerAddress);
    streamConfigMap.put("stream.rocketmq.consumer.type", streamRocketMQConsumerType);
    streamConfigMap.put("stream.rocketmq.consumer.factory.class.name", getRocketMQConsumerFactoryName());
    streamConfigMap.put("stream.rocketmq.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    RocketMQStreamMetadataProvider metadataProvider = new RocketMQStreamMetadataProvider(clientId, streamConfig);
    int numPartitions = metadataProvider.fetchPartitionCount(10000);
    for (int partition = 0; partition < numPartitions; partition++) {
      RocketMQStreamMetadataProvider rocketmqStreamMetadataProvider =
          new RocketMQStreamMetadataProvider(clientId, streamConfig);
      Assert.assertEquals(new LongMsgOffset(0).compareTo(rocketmqStreamMetadataProvider
          .fetchStreamPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000)), 0);
      Assert.assertEquals(new LongMsgOffset(NUM_MSG_PRODUCED_PER_PARTITION).compareTo(rocketmqStreamMetadataProvider
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
      throws Exception {
    String streamType = "rocketmq";
    String streamRocketMQNameServerAddress = _namesrvAddress;
    String streamRocketMQConsumerType = "highlevel";
    String clientId = "clientId";
    String groupId = "groupId";
    String tableNameWithType = "tableName_REALTIME";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.rocketmq.topic.name", topic);
    streamConfigMap.put("stream.rocketmq.nameserver.list", streamRocketMQNameServerAddress);
    streamConfigMap.put("stream.rocketmq.consumer.type", streamRocketMQConsumerType);
    streamConfigMap.put("stream.rocketmq.consumer.factory.class.name", getRocketMQConsumerFactoryName());
    streamConfigMap.put("stream.rocketmq.decoder.class.name", "decoderClass");
    streamConfigMap.put("stream.rocketmq.hlc.group.id", groupId);
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    try (MockedStatic<StreamDecoderProvider> mockedProvider = Mockito.mockStatic(StreamDecoderProvider.class)) {
      mockedProvider.when(() -> StreamDecoderProvider.create(streamConfig, null))
          .thenReturn(new StreamMessageDecoder<byte[]>() {
            @Override
            public GenericRow decode(byte[] payload, GenericRow destination) {
              destination.putValue("", new String(payload));
              return destination;
            }

            @Override
            public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
              return null;
            }

            @Override
            public void init(Map props, Set fieldsToRead, String topicName)
                throws Exception {
            }
          });

      RocketMQStreamMetadataProvider metadataProvider = new RocketMQStreamMetadataProvider(clientId, streamConfig);

      int numPartitions = metadataProvider.fetchPartitionCount(10000);

      final StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
      StreamLevelConsumer
          consumer =
          streamConsumerFactory.createStreamLevelConsumer(clientId, tableNameWithType, null, streamConfig.getGroupId());
      consumer.start();

      try {
        int errCount = 0;
        List<GenericRow> rows = new ArrayList<>(numPartitions * NUM_MSG_PRODUCED_PER_PARTITION);
        while (rows.size() < numPartitions * NUM_MSG_PRODUCED_PER_PARTITION) {
          GenericRow consumedRow = new GenericRow();
          consumedRow = consumer.next(consumedRow);
          if (consumedRow != null) {
            rows.add(consumedRow);
            if (rows.size() % 20 == 0) {
              consumer.commit();
            }
            Assert.assertTrue(((String) consumedRow.getValue("")).startsWith("sample_msg_"));
          } else {
            errCount++;
          }
          if (errCount >= 100) {
            Assert.fail("too many errors when consuming messages. count: " + errCount);
          }
        }
        consumer.commit();

        Assert.assertNull(consumer.next(new GenericRow()));
        Assert.assertEquals(rows.size(), numPartitions * NUM_MSG_PRODUCED_PER_PARTITION);
      } finally {
        consumer.shutdown();
        metadataProvider.close();
      }
    }
  }

  protected String getRocketMQConsumerFactoryName() {
    return RocketMQConsumerFactory.class.getName();
  }
}
