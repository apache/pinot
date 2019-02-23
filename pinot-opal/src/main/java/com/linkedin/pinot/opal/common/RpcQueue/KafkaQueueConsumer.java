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
package com.linkedin.pinot.opal.common.RpcQueue;

import com.google.common.base.Preconditions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public abstract class KafkaQueueConsumer<V> implements QueueConsumer<V> {


  private Properties _consumerProperties;
  private Map<String, List<Integer>> _topicPartitionMap;
  private KafkaConsumer<byte[], V> _consumer;

  public void init(Properties consumerProperties, Map<String, List<Integer>> topicPartitionMap) {
    getLogger().info("starting to init consumer");
    _consumerProperties = consumerProperties;
    _topicPartitionMap = topicPartitionMap;
    Preconditions.checkState(_topicPartitionMap.size() > 0, "topic partition map should not be empty");

    _consumer = new KafkaConsumer<>(consumerProperties);
    List<TopicPartition> subscribePartitions = new ArrayList<>();
    for (Map.Entry<String, List<Integer>> entry: topicPartitionMap.entrySet()) {
      String topic = entry.getKey();
      List<Integer> partitions = entry.getValue();
      if (partitions.size() == 0) {
        getLogger().error("topic {} has 0 partitions given for its input", topic);
      }
      getLogger().info("adding topic {} with partitions {}", topic,
          partitions.stream().map(Object::toString).collect(Collectors.joining(",")));
      for (int partition: partitions) {
        subscribePartitions.add(new TopicPartition(topic, partition));
      }
    }
    _consumer.assign(subscribePartitions);
  }

  public abstract Logger getLogger();

  @Override
  public List<V> getRequests(long timeout, TimeUnit timeUnit) {
    ConsumerRecords<byte[], V> records = getConsumerRecords(timeout, timeUnit);
    List<V> msgList = new ArrayList<>(records.count());
    for (ConsumerRecord<byte[], V> record : records) {
      msgList.add(record.value());
    }
    return msgList;
  }

  public ConsumerRecords<byte[], V> getConsumerRecords(long timeout, TimeUnit timeUnit) {
    return _consumer.poll(timeUnit.toMillis(timeout));
  }

  @Override
  public void ackOffset() {
    _consumer.commitSync();
  }

  public void close() {
    _consumer.close();
  }
}
