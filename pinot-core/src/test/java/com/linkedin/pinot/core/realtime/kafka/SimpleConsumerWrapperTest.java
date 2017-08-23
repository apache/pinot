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

import com.linkedin.pinot.core.realtime.impl.kafka.PinotKafkaConsumer;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaConsumerFactory;
import com.linkedin.pinot.core.realtime.impl.kafka.SimpleConsumerWrapper;
import java.util.Collections;
import java.util.HashMap;
import kafka.api.FetchRequest;
import kafka.api.PartitionFetchInfo;
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
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.collection.immutable.List;

import static org.testng.Assert.assertEquals;


/**
 * Tests for the SimpleConsumerWrapper.
 */
public class SimpleConsumerWrapperTest {
  public class MockSimpleConsumerWrapper extends SimpleConsumerWrapper implements PinotKafkaConsumer {
    public MockSimpleConsumerWrapper(String bootstrapNodes, String clientId, String topic, int partition,
        long connectTimeoutMillis) {
      super(bootstrapNodes, clientId, topic, partition, connectTimeoutMillis);
    }
    @Override
    public SimpleConsumer makeSimpleConsumer() {
      return new MockSimpleConsumer("node1", 1234, 1000, 1000, "clientId", 0);
    }
  }
  public class MockKafkaSimpleConsumerFactory implements KafkaConsumerFactory {

    @Override
    public PinotKafkaConsumer buildConsumer(String bootstrapNodes, String clientId, String topic, int partition,
        long connectTimeoutMillis) {
      return new MockSimpleConsumerWrapper(bootstrapNodes, clientId, topic, partition, connectTimeoutMillis);
    }
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
    private int _index;
    private String _topicName = "topic";
    private String host = "node1";
    private int port = 1234;
    private BrokerEndPoint[] brokerArray;
    public MockSimpleConsumer(String host, int port, int soTimeout, int bufferSize, String clientId, int index) {
      super(host, port, soTimeout, bufferSize, clientId);
      this._index = index;

      brokerArray = new BrokerEndPoint[1];
      brokerArray[0] = new BrokerEndPoint(0, host, port);
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

        if (!topicAndPartition.topic().equals(_topicName)) {
          errorMap.put(topicAndPartition, Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
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
      kafka.api.TopicMetadata[] topicMetadataArray = new kafka.api.TopicMetadata[topics.size()];
      int partitionCount = 2;

      for (int i = 0; i < topicMetadataArray.length; i++) {
        String topic = topics.get(i);
        if (!topic.equals(_topicName)) {
          topicMetadataArray[i] = new kafka.api.TopicMetadata(topic, null, Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        } else {
          kafka.api.PartitionMetadata[] partitionMetadataArray = new kafka.api.PartitionMetadata[partitionCount];
          for (int j = 0; j < partitionCount; j++) {
            java.util.List<BrokerEndPoint> emptyJavaList = Collections.emptyList();
            List<BrokerEndPoint> emptyScalaList = JavaConversions.asScalaBuffer(emptyJavaList).toList();
            partitionMetadataArray[j] = new kafka.api.PartitionMetadata(j, Some
                .apply(brokerArray[0]),
                emptyScalaList, emptyScalaList, Errors.NONE.code());
          }

          Seq<kafka.api.PartitionMetadata> partitionsMetadata = List.fromArray(partitionMetadataArray);
          topicMetadataArray[i] = new kafka.api.TopicMetadata(topic, partitionsMetadata, Errors.NONE.code());
        }
      }

      Seq<BrokerEndPoint> brokers = List.fromArray(brokerArray);
      Seq<kafka.api.TopicMetadata> topicsMetadata = List.fromArray(topicMetadataArray);

      return new TopicMetadataResponse(new kafka.api.TopicMetadataResponse(brokers, topicsMetadata, -1));
    }
  }

  @Test
  public void testGetPartitionCount() {
    KafkaConsumerFactory kafkaConsumerFactory = new MockKafkaSimpleConsumerFactory();
    SimpleConsumerWrapper consumerWrapper = (MockSimpleConsumerWrapper) kafkaConsumerFactory.buildConsumer(
        "node1:1234,node2:2345", "clientId", "topic", 1, 123456L);

    assertEquals(consumerWrapper.getPartitionCount("topic", 12345L), 2);
  }

  @Test
  public void testFetchMessages()
      throws Exception {
    KafkaConsumerFactory kafkaConsumerFactory = new MockKafkaSimpleConsumerFactory();
    SimpleConsumerWrapper consumerWrapper = (MockSimpleConsumerWrapper) kafkaConsumerFactory.buildConsumer(
        "node1:1234,node2:2345", "clientId", "topic", 1, 123456L);

    consumerWrapper.fetchMessages(12345L, 23456L, 10000);
  }

  @Test(enabled = false)
  public void testFetchOffsets()
      throws Exception {
//    KafkaConsumerFactory kafkaConsumerFactory = new MockKafkaSimpleConsumerFactory();
//    PinotKafkaConsumer consumerWrapper = kafkaConsumerFactory.buildConsumer("node1:1234,node2:2345", "clientId", "topic", 1, 123456L);
//
//    consumerWrapper.fetchPartitionOffset("smallest", 10000);
  }
}

