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

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamDecoderProvider;
import org.apache.pinot.spi.stream.StreamLevelConsumer;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of a {@link StreamLevelConsumer} which consumes from the kafka stream
 */
public class KafkaStreamLevelConsumer implements StreamLevelConsumer {

  private StreamMessageDecoder _messageDecoder;
  private Logger INSTANCE_LOGGER;

  private String _clientId;
  private String _tableAndStreamName;

  private StreamConfig _streamConfig;
  private KafkaStreamLevelStreamConfig _kafkaStreamLevelStreamConfig;

  private KafkaConsumer<Bytes, Bytes> consumer;
  private ConsumerRecords<Bytes, Bytes> consumerRecords;
  private Iterator<ConsumerRecord<Bytes, Bytes>> kafkaIterator;
  private Map<Integer, Long> consumerOffsets = new HashMap<>(); // tracking current consumed records offsets.

  private long lastLogTime = 0;
  private long lastCount = 0;
  private long currentCount = 0L;

  public KafkaStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig,
      Set<String> sourceFields, String groupId) {
    _clientId = clientId;
    _streamConfig = streamConfig;
    _kafkaStreamLevelStreamConfig = new KafkaStreamLevelStreamConfig(streamConfig, tableName, groupId);

    _messageDecoder = StreamDecoderProvider.create(streamConfig, sourceFields);

    _tableAndStreamName = tableName + "-" + streamConfig.getTopicName();
    INSTANCE_LOGGER = LoggerFactory
        .getLogger(KafkaStreamLevelConsumer.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
    INSTANCE_LOGGER.info("KafkaStreamLevelConsumer: streamConfig : {}", _streamConfig);
  }

  @Override
  public void start() throws Exception {
    consumer = KafkaStreamLevelConsumerManager.acquireKafkaConsumerForConfig(_kafkaStreamLevelStreamConfig);
  }

  private void updateKafkaIterator() {
    consumerRecords = consumer.poll(Duration.ofMillis(_streamConfig.getFetchTimeoutMillis()));
    kafkaIterator = consumerRecords.iterator();
  }

  private void resetOffsets() {
    for (int partition : consumerOffsets.keySet()) {
      long offsetToSeek = consumerOffsets.get(partition);
      consumer.seek(new TopicPartition(_streamConfig.getTopicName(), partition), offsetToSeek);
    }
  }

  @Override
  public GenericRow next(GenericRow destination) {
    if (kafkaIterator == null || !kafkaIterator.hasNext()) {
      updateKafkaIterator();
    }
    if (kafkaIterator.hasNext()) {
      try {
        final ConsumerRecord<Bytes, Bytes> record = kafkaIterator.next();
        updateOffsets(record.partition(), record.offset());
        destination = _messageDecoder.decode(record.value().get(), destination);

        ++currentCount;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - lastLogTime > 60000 || currentCount - lastCount >= 100000) {
          if (lastCount == 0) {
            INSTANCE_LOGGER.info("Consumed {} events from kafka stream {}", currentCount, _streamConfig.getTopicName());
          } else {
            INSTANCE_LOGGER.info("Consumed {} events from kafka stream {} (rate:{}/s)", currentCount - lastCount,
                _streamConfig.getTopicName(), (float) (currentCount - lastCount) * 1000 / (now - lastLogTime));
          }
          lastCount = currentCount;
          lastLogTime = now;
        }
        return destination;
      } catch (Exception e) {
        INSTANCE_LOGGER.warn("Caught exception while consuming events", e);
        throw e;
      }
    }
    return null;
  }

  private void updateOffsets(int partition, long offset) {
    consumerOffsets.put(partition, offset + 1);
  }

  @Override
  public void commit() {
    consumer.commitSync(getOffsetsMap());
    // Since the lastest batch may not be consumed fully, so we need to reset kafka consumer's offset.
    resetOffsets();
    consumerOffsets.clear();
  }

  private Map<TopicPartition, OffsetAndMetadata> getOffsetsMap() {
    Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();
    for (Integer partition : consumerOffsets.keySet()) {
      offsetsMap.put(new TopicPartition(_streamConfig.getTopicName(), partition),
          new OffsetAndMetadata(consumerOffsets.get(partition)));
    }
    return offsetsMap;
  }

  @Override
  public void shutdown() throws Exception {
    if (consumer != null) {
      // If offsets commit is not succeed, then reset the offsets here.
      resetOffsets();
      KafkaStreamLevelConsumerManager.releaseKafkaConsumer(consumer);
      consumer = null;
    }
  }
}
