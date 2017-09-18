/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.core.realtime.kafka;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaSimpleConsumerFactory;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
import org.testng.annotations.Test;
import scala.Some;
import scala.Tuple2;
import scala.collection.Iterable;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.List;

import static org.testng.Assert.assertEquals;


/**
 * Tests for the SimpleConsumerWrapper.
 */
public class SimpleConsumerWrapperTest {
  public class MockKafkaSimpleConsumerFactory implements KafkaSimpleConsumerFactory {
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
      this.partitionStartOffsets = partitionStartOffsets;
      this.partitionEndOffsets = partitionEndOffsets;
      this.partitionLeaderIndices = partitionLeaderIndices;
      partitionCount = partitionStartOffsets.length;

      this.topicName = topicName;
    }

    private String[] hosts;
    private int[] ports;
    private long[] partitionStartOffsets;
    private long[] partitionEndOffsets;
    private int[] partitionLeaderIndices;
    private int brokerCount;
    private int partitionCount;
    private String topicName;
    private BrokerEndPoint[] brokerArray;

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

        while(requestInfo.headOption().isDefined()) {
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
              partitionMetadataArray[j] = new PartitionMetadata(j, Some.apply(brokerArray[partitionLeaderIndices[j]]),
                  emptyScalaList, emptyScalaList, Errors.NONE.code());
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
  public void testGetPartitionCount() {
    MockKafkaSimpleConsumerFactory simpleConsumerFactory = new MockKafkaSimpleConsumerFactory(
        new String[] { "abcd", "bcde" },
        new int[] { 1234, 2345 },
        new long[] { 12345L, 23456L },
        new long[] { 23456L, 34567L },
        new int[] { 0, 1 },
        "theTopic"
    );
    SimpleConsumerWrapper consumerWrapper = new SimpleConsumerWrapper(
        simpleConsumerFactory, "abcd:1234,bcde:2345", "clientId", 10000L);
    assertEquals(consumerWrapper.getPartitionCount("theTopic", 10000L), 2);
  }

  @Test
  public void testFetchMessages() throws Exception {
    MockKafkaSimpleConsumerFactory simpleConsumerFactory = new MockKafkaSimpleConsumerFactory(
        new String[] { "abcd", "bcde" },
        new int[] { 1234, 2345 },
        new long[] { 12345L, 23456L },
        new long[] { 23456L, 34567L },
        new int[] { 0, 1 },
        "theTopic"
    );
    SimpleConsumerWrapper consumerWrapper = new SimpleConsumerWrapper(simpleConsumerFactory, "abcd:1234,bcde:2345", "clientId", "theTopic", 0, 10000L);
    consumerWrapper.fetchMessages(12345L, 23456L, 10000);
  }

  @Test(enabled = false)
  public void testFetchOffsets() throws Exception {
    MockKafkaSimpleConsumerFactory simpleConsumerFactory = new MockKafkaSimpleConsumerFactory(
        new String[] { "abcd", "bcde" },
        new int[] { 1234, 2345 },
        new long[] { 12345L, 23456L },
        new long[] { 23456L, 34567L },
        new int[] { 0, 1 },
        "theTopic"
    );
    SimpleConsumerWrapper consumerWrapper = new SimpleConsumerWrapper(simpleConsumerFactory, "abcd:1234,bcde:2345", "clientId", "theTopic", 0, 10000L);
    consumerWrapper.fetchPartitionOffset("smallest", 10000);

  }
}
