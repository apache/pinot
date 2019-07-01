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
package org.apache.pinot.core.realtime.impl.kafka2;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pinot.core.realtime.impl.kafka2.utils.MiniKafkaCluster;
import org.apache.pinot.core.realtime.stream.OffsetCriteria;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for the KafkaPartitionLevelConsumer.
 */
public class KafkaPartitionLevelConsumerTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumerTest.class);
  private static final long STABILIZE_SLEEP_DELAYS = 3000;
  private static final String TEST_TOPIC_1 = "foo";
  private static final String TEST_TOPIC_2 = "bar";
  private static final int NUM_MSG_PRODUCED = 1000;

  private static MiniKafkaCluster kafkaCluster;
  private static String brokerAddress;

  @BeforeClass
  public static void setup()
      throws Exception {
    kafkaCluster = new MiniKafkaCluster.Builder().newServer("0").build();
    LOGGER.info("Trying to start MiniKafkaCluster");
    kafkaCluster.start();
    brokerAddress = getKakfaBroker();
    kafkaCluster.createTopic(TEST_TOPIC_1, 1, 1);
    kafkaCluster.createTopic(TEST_TOPIC_2, 2, 1);
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
    produceMsgToKafka();
    Thread.sleep(STABILIZE_SLEEP_DELAYS);
  }

  private static void produceMsgToKafka() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKakfaBroker());
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer p = new KafkaProducer<>(props);
    for (int i = 0; i < NUM_MSG_PRODUCED; i++) {
      p.send(new ProducerRecord(TEST_TOPIC_1, "sample_msg_" + i));
      p.send(new ProducerRecord(TEST_TOPIC_2, "sample_msg_" + i));
    }
  }

  private static String getKakfaBroker() {
    return "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
  }

  @AfterClass
  public static void shutDown()
      throws Exception {
    kafkaCluster.deleteTopic(TEST_TOPIC_1);
    kafkaCluster.deleteTopic(TEST_TOPIC_2);
    kafkaCluster.close();
  }

  @Test
  public void testBuildConsumer()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", Kafka2ConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfigMap.put("stream.kafka.fetcher.size", "10000");
    streamConfigMap.put("stream.kafka.fetcher.minBytes", "20000");
    StreamConfig streamConfig = new StreamConfig(streamConfigMap);

    Kafka2PartitionLevelStreamMetadataProvider streamMetadataProvider =
        new Kafka2PartitionLevelStreamMetadataProvider(clientId, streamConfig);

    // test default value
    Kafka2PartitionLevelPartitionLevelConsumer kafkaSimpleStreamConsumer =
        new Kafka2PartitionLevelPartitionLevelConsumer(clientId, streamConfig, 0);
    kafkaSimpleStreamConsumer.fetchMessages(12345L, 23456L, 10000);

    Assert.assertEquals(Kafka2StreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        kafkaSimpleStreamConsumer.getKafka2PartitionLevelStreamConfig().getKafkaBufferSize());
    Assert.assertEquals(Kafka2StreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        kafkaSimpleStreamConsumer.getKafka2PartitionLevelStreamConfig().getKafkaSocketTimeout());

    // test parsing values
    Assert.assertEquals(10000,
        kafkaSimpleStreamConsumer.getKafka2PartitionLevelStreamConfig().getKafkaFetcherSizeBytes());
    Assert
        .assertEquals(20000, kafkaSimpleStreamConsumer.getKafka2PartitionLevelStreamConfig().getKafkaFetcherMinBytes());

    // test user defined values
    streamConfigMap.put("stream.kafka.buffer.size", "100");
    streamConfigMap.put("stream.kafka.socket.timeout", "1000");
    streamConfig = new StreamConfig(streamConfigMap);
    kafkaSimpleStreamConsumer = new Kafka2PartitionLevelPartitionLevelConsumer(clientId, streamConfig, 0);
    kafkaSimpleStreamConsumer.fetchMessages(12345L, 23456L, 10000);
    Assert.assertEquals(100, kafkaSimpleStreamConsumer.getKafka2PartitionLevelStreamConfig().getKafkaBufferSize());
    Assert.assertEquals(1000, kafkaSimpleStreamConsumer.getKafka2PartitionLevelStreamConfig().getKafkaSocketTimeout());
  }

  @Test
  public void testGetPartitionCount() {
    String streamType = "kafka";
    String streamKafkaBrokerList = "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC_1);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", Kafka2ConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(streamConfigMap);

    Kafka2PartitionLevelStreamMetadataProvider streamMetadataProvider =
        new Kafka2PartitionLevelStreamMetadataProvider(clientId, streamConfig);
    Assert.assertEquals(streamMetadataProvider.fetchPartitionCount(10000L), 1);

    streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", TEST_TOPIC_2);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", Kafka2ConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfig = new StreamConfig(streamConfigMap);

    streamMetadataProvider = new Kafka2PartitionLevelStreamMetadataProvider(clientId, streamConfig);
    Assert.assertEquals(streamMetadataProvider.fetchPartitionCount(10000L), 2);
  }

  @Test
  public void testFetchMessages()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", Kafka2ConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(streamConfigMap);

    int partition = 0;
    Kafka2PartitionLevelPartitionLevelConsumer kafkaSimpleStreamConsumer =
        new Kafka2PartitionLevelPartitionLevelConsumer(clientId, streamConfig, partition);
    kafkaSimpleStreamConsumer.fetchMessages(12345L, 23456L, 10000);
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
    String streamKafkaBrokerList = "127.0.0.1:" + kafkaCluster.getKafkaServerPort(0);
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", topic);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap.put("stream.kafka.consumer.factory.class.name", Kafka2ConsumerFactory.class.getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(streamConfigMap);

    int numPartitions =
        new Kafka2PartitionLevelStreamMetadataProvider(clientId, streamConfig).fetchPartitionCount(10000);
    for (int partition = 0; partition < numPartitions; partition++) {
      Kafka2PartitionLevelStreamMetadataProvider kafkaStreamMetadataProvider =
          new Kafka2PartitionLevelStreamMetadataProvider(clientId, streamConfig, partition);
      Assert.assertEquals(0, kafkaStreamMetadataProvider
          .fetchPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000));
      Assert.assertEquals(NUM_MSG_PRODUCED / numPartitions, kafkaStreamMetadataProvider
          .fetchPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest(), 10000));
    }
  }
}
