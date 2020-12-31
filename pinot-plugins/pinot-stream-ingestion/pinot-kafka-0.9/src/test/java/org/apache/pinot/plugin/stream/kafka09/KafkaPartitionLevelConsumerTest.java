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
package org.apache.pinot.plugin.stream.kafka09;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import kafka.api.FetchRequest;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pinot.plugin.stream.kafka.KafkaStreamConfigProperties;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Some;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.List;


/**
 * Tests for the KafkaPartitionLevelConsumer.
 */
public class KafkaPartitionLevelConsumerTest {
  public class MockKafkaSimpleConsumerFactory implements KafkaSimpleConsumerFactory {

    private String[] hosts;
    private int[] ports;
    private int[] partitionLeaderIndices;
    private int brokerCount;
    private int partitionCount;
    private String topicName;
    private BrokerEndPoint[] brokerArray;

    public MockKafkaSimpleConsumerFactory(String[] hosts, int[] ports, long[] partitionStartOffsets,
        long[] partitionEndOffsets, int[] partitionLeaderIndices, String topicName) {
      Preconditions.checkArgument(hosts.length == ports.length);
      this.hosts = hosts;
      this.ports = ports;
      brokerCount = hosts.length;

      brokerArray = new BrokerEndPoint[brokerCount];
      for (int i = 0; i < brokerCount; i++) {
        brokerArray[i] = new BrokerEndPoint(i, hosts[i], ports[i]);
      }

      Preconditions.checkArgument(partitionStartOffsets.length == partitionEndOffsets.length);
      Preconditions.checkArgument(partitionStartOffsets.length == partitionLeaderIndices.length);
      this.partitionLeaderIndices = partitionLeaderIndices;
      partitionCount = partitionStartOffsets.length;

      this.topicName = topicName;
    }

    private class MockFetchResponse extends FetchResponse {
      java.util.Map<TopicAndPartition, Short> errorMap;

      public MockFetchResponse(java.util.Map<TopicAndPartition, Short> errorMap) {
        super(null);
        this.errorMap = errorMap;
      }

      @Override
      public ByteBufferMessageSet messageSet(String topic, int partition) {
        if (errorMap.containsKey(new TopicAndPartition(topic, partition))) {
          throw new IllegalArgumentException();
        } else {
          // TODO Maybe generate dummy messages here?
          return new ByteBufferMessageSet(Collections.<Message>emptyList());
        }
      }

      @Override
      public short errorCode(String topic, int partition) {
        TopicAndPartition key = new TopicAndPartition(topic, partition);
        if (errorMap.containsKey(key)) {
          return errorMap.get(key);
        } else {
          return Errors.NONE.code();
        }
      }

      @Override
      public long highWatermark(String topic, int partition) {
        return 0L;
      }

      public boolean hasError() {
        return !errorMap.isEmpty();
      }
    }

    private class MockSimpleConsumer extends SimpleConsumer {
      private int index;

      public MockSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId, int index) {
        super(host, port, soTimeout, bufferSize, clientId);
        this.index = index;
      }

      @Override
      public FetchResponse fetch(FetchRequest request) {
        scala.collection.Traversable<Tuple2<TopicAndPartition, PartitionFetchInfo>> requestInfo = request.requestInfo();
        java.util.Map<TopicAndPartition, Short> errorMap = new HashMap<>();

        while (requestInfo.headOption().isDefined()) {
          // jfim: IntelliJ erroneously thinks the following line is an incompatible type error, but it's only because
          // it doesn't understand scala covariance when called from Java (ie. it thinks head() is of type A even though
          // it's really of type Tuple2[TopicAndPartition, PartitionFetchInfo])
          Tuple2<TopicAndPartition, PartitionFetchInfo> t2 = requestInfo.head();
          TopicAndPartition topicAndPartition = t2._1();
          PartitionFetchInfo partitionFetchInfo = t2._2();

          if (!topicAndPartition.topic().equals(topicName)) {
            errorMap.put(topicAndPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
          } else if (partitionLeaderIndices.length < topicAndPartition.partition()) {
            errorMap.put(topicAndPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
          } else if (partitionLeaderIndices[topicAndPartition.partition()] != index) {
            errorMap.put(topicAndPartition, Errors.NOT_LEADER_FOR_PARTITION.code());
          } else {
            // Do nothing, we'll generate a fake message
          }

          requestInfo = requestInfo.tail();
        }

        return new MockFetchResponse(errorMap);
      }

      @Override
      public FetchResponse fetch(kafka.javaapi.FetchRequest request) {
        throw new RuntimeException("Unimplemented");
      }

      @Override
      public OffsetResponse getOffsetsBefore(OffsetRequest request) {
        throw new RuntimeException("Unimplemented!");
      }

      @Override
      public TopicMetadataResponse send(TopicMetadataRequest request) {
        java.util.List<String> topics = request.topics();
        TopicMetadata[] topicMetadataArray = new TopicMetadata[topics.size()];

        for (int i = 0; i < topicMetadataArray.length; i++) {
          String topic = topics.get(i);
          if (!topic.equals(topicName)) {
            topicMetadataArray[i] = new TopicMetadata(topic, null, Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
          } else {
            PartitionMetadata[] partitionMetadataArray = new PartitionMetadata[partitionCount];
            for (int j = 0; j < partitionCount; j++) {
              java.util.List<BrokerEndPoint> emptyJavaList = Collections.emptyList();
              List<BrokerEndPoint> emptyScalaList = JavaConversions.asScalaBuffer(emptyJavaList).toList();
              partitionMetadataArray[j] =
                  new PartitionMetadata(j, Some.apply(brokerArray[partitionLeaderIndices[j]]), emptyScalaList,
                      emptyScalaList, Errors.NONE.code());
            }

            Seq<PartitionMetadata> partitionsMetadata = List.fromArray(partitionMetadataArray);
            topicMetadataArray[i] = new TopicMetadata(topic, partitionsMetadata, Errors.NONE.code());
          }
        }

        Seq<BrokerEndPoint> brokers = List.fromArray(brokerArray);
        Seq<TopicMetadata> topicsMetadata = List.fromArray(topicMetadataArray);

        return new TopicMetadataResponse(new kafka.api.TopicMetadataResponse(brokers, topicsMetadata, -1));
      }
    }

    @Override
    public SimpleConsumer buildSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
      for (int i = 0; i < brokerCount; i++) {
        if (hosts[i].equalsIgnoreCase(host) && ports[i] == port) {
          return new MockSimpleConsumer(host, port, soTimeout, bufferSize, clientId, i);
        }
      }

      throw new RuntimeException("No such host/port");
    }
  }

  @Test
  public void testBuildConsumer()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = "abcd:1234,bcde:2345";
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "table_REALTIME";

    MockKafkaSimpleConsumerFactory mockKafkaSimpleConsumerFactory =
        new MockKafkaSimpleConsumerFactory(new String[]{"abcd", "bcde"}, new int[]{1234, 2345},
            new long[]{12345L, 23456L}, new long[]{23456L, 34567L}, new int[]{0, 1}, streamKafkaTopicName);

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap
        .put("stream.kafka.consumer.factory.class.name", mockKafkaSimpleConsumerFactory.getClass().getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    streamConfigMap.put("stream.kafka.fetcher.size", "10000");
    streamConfigMap.put("stream.kafka.fetcher.minBytes", "20000");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    KafkaStreamMetadataProvider streamMetadataProvider =
        new KafkaStreamMetadataProvider(clientId, streamConfig, mockKafkaSimpleConsumerFactory);

    // test default value
    KafkaPartitionLevelConsumer kafkaSimpleStreamConsumer =
        new KafkaPartitionLevelConsumer(clientId, streamConfig, 0, mockKafkaSimpleConsumerFactory);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), new LongMsgOffset(23456L), 10000);

    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_BUFFER_SIZE_DEFAULT,
        kafkaSimpleStreamConsumer.getSimpleConsumer().bufferSize());
    Assert.assertEquals(KafkaStreamConfigProperties.LowLevelConsumer.KAFKA_SOCKET_TIMEOUT_DEFAULT,
        kafkaSimpleStreamConsumer.getSimpleConsumer().soTimeout());

    // test parsing values
    Assert.assertEquals(10000, kafkaSimpleStreamConsumer.getFetchRequestSize());
    Assert.assertEquals(20000, kafkaSimpleStreamConsumer.getFetchRequestMinBytes());

    // test user defined values
    streamConfigMap.put("stream.kafka.buffer.size", "100");
    streamConfigMap.put("stream.kafka.socket.timeout", "1000");
    streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);
    kafkaSimpleStreamConsumer =
        new KafkaPartitionLevelConsumer(clientId, streamConfig, 0, mockKafkaSimpleConsumerFactory);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), new LongMsgOffset(23456L), 10000);
    Assert.assertEquals(100, kafkaSimpleStreamConsumer.getSimpleConsumer().bufferSize());
    Assert.assertEquals(1000, kafkaSimpleStreamConsumer.getSimpleConsumer().soTimeout());
  }

  @Test
  public void testGetPartitionCount() throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = "abcd:1234,bcde:2345";
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "table_REALTIME";

    MockKafkaSimpleConsumerFactory mockKafkaSimpleConsumerFactory =
        new MockKafkaSimpleConsumerFactory(new String[]{"abcd", "bcde"}, new int[]{1234, 2345},
            new long[]{12345L, 23456L}, new long[]{23456L, 34567L}, new int[]{0, 1}, streamKafkaTopicName);

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap
        .put("stream.kafka.consumer.factory.class.name", mockKafkaSimpleConsumerFactory.getClass().getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    KafkaStreamMetadataProvider streamMetadataProvider =
        new KafkaStreamMetadataProvider(clientId, streamConfig, mockKafkaSimpleConsumerFactory);
    Assert.assertEquals(streamMetadataProvider.getPartitionGroupInfoList(Collections.emptyList(), 10000L), 2);
  }

  @Test
  public void testFetchMessages()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = "abcd:1234,bcde:2345";
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "table_REALTIME";

    MockKafkaSimpleConsumerFactory mockKafkaSimpleConsumerFactory =
        new MockKafkaSimpleConsumerFactory(new String[]{"abcd", "bcde"}, new int[]{1234, 2345},
            new long[]{12345L, 23456L}, new long[]{23456L, 34567L}, new int[]{0, 1}, streamKafkaTopicName);

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap
        .put("stream.kafka.consumer.factory.class.name", mockKafkaSimpleConsumerFactory.getClass().getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    int partition = 0;
    KafkaPartitionLevelConsumer kafkaSimpleStreamConsumer =
        new KafkaPartitionLevelConsumer(clientId, streamConfig, partition, mockKafkaSimpleConsumerFactory);
    kafkaSimpleStreamConsumer.fetchMessages(new LongMsgOffset(12345L), new LongMsgOffset(23456L), 10000);
  }

  @Test(enabled = false)
  public void testFetchOffsets()
      throws Exception {
    String streamType = "kafka";
    String streamKafkaTopicName = "theTopic";
    String streamKafkaBrokerList = "abcd:1234,bcde:2345";
    String streamKafkaConsumerType = "simple";
    String clientId = "clientId";
    String tableNameWithType = "table_REALTIME";

    MockKafkaSimpleConsumerFactory mockKafkaSimpleConsumerFactory =
        new MockKafkaSimpleConsumerFactory(new String[]{"abcd", "bcde"}, new int[]{1234, 2345},
            new long[]{12345L, 23456L}, new long[]{23456L, 34567L}, new int[]{0, 1}, streamKafkaTopicName);

    Map<String, String> streamConfigMap = new HashMap<>();
    streamConfigMap.put("streamType", streamType);
    streamConfigMap.put("stream.kafka.topic.name", streamKafkaTopicName);
    streamConfigMap.put("stream.kafka.broker.list", streamKafkaBrokerList);
    streamConfigMap.put("stream.kafka.consumer.type", streamKafkaConsumerType);
    streamConfigMap
        .put("stream.kafka.consumer.factory.class.name", mockKafkaSimpleConsumerFactory.getClass().getName());
    streamConfigMap.put("stream.kafka.decoder.class.name", "decoderClass");
    StreamConfig streamConfig = new StreamConfig(tableNameWithType, streamConfigMap);

    int partition = 0;
    KafkaStreamMetadataProvider kafkaStreamMetadataProvider =
        new KafkaStreamMetadataProvider(clientId, streamConfig, partition, mockKafkaSimpleConsumerFactory);
    kafkaStreamMetadataProvider
        .fetchStreamPartitionOffset(new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest(), 10000);
  }
}
