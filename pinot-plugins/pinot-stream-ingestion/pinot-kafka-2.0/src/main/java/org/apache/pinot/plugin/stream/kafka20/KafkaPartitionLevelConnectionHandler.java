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
package org.apache.pinot.plugin.stream.kafka20;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.stream.StreamConfig;


/**
 * KafkaPartitionLevelConnectionHandler provides low level APIs to access Kafka partition level information.
 * E.g. partition counts, offsets per partition.
 *
 */
public abstract class KafkaPartitionLevelConnectionHandler {

  protected final KafkaPartitionLevelStreamConfig _config;
  protected final String _clientId;
  protected final int _partition;
  protected final String _topic;
  protected final Consumer<String, Bytes> _consumer;
  protected final TopicPartition _topicPartition;

  public KafkaPartitionLevelConnectionHandler(String clientId, StreamConfig streamConfig, int partition) {
    _config = new KafkaPartitionLevelStreamConfig(streamConfig);
    _clientId = clientId;
    _partition = partition;
    _topic = _config.getKafkaTopicName();
    Properties consumerProp = new Properties();
    consumerProp.putAll(streamConfig.getStreamConfigsMap());
    consumerProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _config.getBootstrapHosts());
    consumerProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BytesDeserializer.class.getName());
    if (_config.getKafkaIsolationLevel() != null) {
      consumerProp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, _config.getKafkaIsolationLevel());
    }
    _consumer = new KafkaConsumer<>(consumerProp);
    _topicPartition = new TopicPartition(_topic, _partition);
    _consumer.assign(Collections.singletonList(_topicPartition));
  }

  public void close()
      throws IOException {
    _consumer.close();
  }

  @VisibleForTesting
  protected KafkaPartitionLevelStreamConfig getKafkaPartitionLevelStreamConfig() {
    return _config;
  }
}
