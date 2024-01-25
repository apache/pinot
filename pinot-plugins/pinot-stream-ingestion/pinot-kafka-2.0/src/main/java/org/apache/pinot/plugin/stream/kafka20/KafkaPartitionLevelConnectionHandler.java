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
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * KafkaPartitionLevelConnectionHandler provides low level APIs to access Kafka partition level information.
 * E.g. partition counts, offsets per partition.
 *
 */
public abstract class KafkaPartitionLevelConnectionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConnectionHandler.class);
  protected final KafkaPartitionLevelStreamConfig _config;
  protected final String _clientId;
  protected final int _partition;
  protected final String _topic;
  protected final Consumer<String, Bytes> _consumer;
  protected final TopicPartition _topicPartition;
  protected final KafkaMetadataExtractor _kafkaMetadataExtractor;

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
    consumerProp.put(ConsumerConfig.CLIENT_ID_CONFIG, _clientId);
    KafkaSSLUtils.initSSL(consumerProp);
    _consumer = createConsumer(consumerProp);
    _topicPartition = new TopicPartition(_topic, _partition);
    _consumer.assign(Collections.singletonList(_topicPartition));
    _kafkaMetadataExtractor = KafkaMetadataExtractor.build(_config.isPopulateMetadata());
  }

  private Consumer<String, Bytes> createConsumer(Properties consumerProp) {
    // Creation of the KafkaConsumer can fail for multiple reasons including DNS issues.
    // We arbitrarily chose 5 retries with 2 seconds sleep in between retries. 10 seconds total felt
    // like a good balance of not waiting too long for a retry, but also not retrying too many times.
    int maxTries = 5;
    int tries = 0;
    while (true) {
      try {
        return new KafkaConsumer<>(consumerProp);
      } catch (KafkaException e) {
        tries++;
        if (tries >= maxTries) {
          LOGGER.error("Caught exception while creating Kafka consumer, giving up", e);
          throw e;
        }
        LOGGER.warn("Caught exception while creating Kafka consumer, retrying {}/{}", tries, maxTries, e);
        // We are choosing to sleepUniterruptibly here because other parts of the Kafka consumer code do this
        // as well. We don't want random interrupts to cause us to fail to create the consumer and have the table
        // stuck in ERROR state.
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
      }
    }
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
