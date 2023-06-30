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
@Deprecated(since = "Pinot no longer support high level consumer model since v0.12.*")
public class KafkaStreamLevelConsumer implements StreamLevelConsumer {

  private final StreamMessageDecoder _messageDecoder;
  private final Logger _instanceLogger;

  private final StreamConfig _streamConfig;
  private final KafkaStreamLevelStreamConfig _kafkaStreamLevelStreamConfig;

  private KafkaConsumer<Bytes, Bytes> _consumer;
  private ConsumerRecords<Bytes, Bytes> _consumerRecords;
  private Iterator<ConsumerRecord<Bytes, Bytes>> _kafkaIterator;
  private Map<Integer, Long> _consumerOffsets = new HashMap<>(); // tracking current consumed records offsets.

  private long _lastLogTime = 0;
  private long _lastCount = 0;
  private long _currentCount = 0L;

  public KafkaStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig,
      Set<String> sourceFields, String groupId) {
    _streamConfig = streamConfig;
    _kafkaStreamLevelStreamConfig = new KafkaStreamLevelStreamConfig(streamConfig, tableName, groupId);

    _messageDecoder = StreamDecoderProvider.create(streamConfig, sourceFields);
    _instanceLogger = LoggerFactory
        .getLogger(KafkaStreamLevelConsumer.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
    _instanceLogger.info("KafkaStreamLevelConsumer: streamConfig : {}", _streamConfig);
  }

  @Override
  public void start()
      throws Exception {
    _consumer = KafkaStreamLevelConsumerManager.acquireKafkaConsumerForConfig(_kafkaStreamLevelStreamConfig);
  }

  private void updateKafkaIterator() {
    _consumerRecords = _consumer.poll(Duration.ofMillis(_streamConfig.getFetchTimeoutMillis()));
    _kafkaIterator = _consumerRecords.iterator();
  }

  private void resetOffsets() {
    for (int partition : _consumerOffsets.keySet()) {
      long offsetToSeek = _consumerOffsets.get(partition);
      _consumer.seek(new TopicPartition(_streamConfig.getTopicName(), partition), offsetToSeek);
    }
  }

  @Override
  public GenericRow next(GenericRow destination) {
    if (_kafkaIterator == null || !_kafkaIterator.hasNext()) {
      updateKafkaIterator();
    }
    if (_kafkaIterator.hasNext()) {
      try {
        final ConsumerRecord<Bytes, Bytes> record = _kafkaIterator.next();
        updateOffsets(record.partition(), record.offset());
        destination = _messageDecoder.decode(record.value().get(), destination);

        _currentCount++;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - _lastLogTime > 60000 || _currentCount - _lastCount >= 100000) {
          if (_lastCount == 0) {
            _instanceLogger
                .info("Consumed {} events from kafka stream {}", _currentCount, _streamConfig.getTopicName());
          } else {
            _instanceLogger.info("Consumed {} events from kafka stream {} (rate:{}/s)", _currentCount - _lastCount,
                _streamConfig.getTopicName(), (float) (_currentCount - _lastCount) * 1000 / (now - _lastLogTime));
          }
          _lastCount = _currentCount;
          _lastLogTime = now;
        }
        return destination;
      } catch (Exception e) {
        _instanceLogger.warn("Caught exception while consuming events", e);
        throw e;
      }
    }
    return null;
  }

  private void updateOffsets(int partition, long offset) {
    _consumerOffsets.put(partition, offset + 1);
  }

  @Override
  public void commit() {
    _consumer.commitSync(getOffsetsMap());
    // Since the lastest batch may not be consumed fully, so we need to reset kafka consumer's offset.
    resetOffsets();
    _consumerOffsets.clear();
  }

  private Map<TopicPartition, OffsetAndMetadata> getOffsetsMap() {
    Map<TopicPartition, OffsetAndMetadata> offsetsMap = new HashMap<>();
    for (Integer partition : _consumerOffsets.keySet()) {
      offsetsMap.put(new TopicPartition(_streamConfig.getTopicName(), partition),
          new OffsetAndMetadata(_consumerOffsets.get(partition)));
    }
    return offsetsMap;
  }

  @Override
  public void shutdown()
      throws Exception {
    if (_consumer != null) {
      // If offsets commit is not succeed, then reset the offsets here.
      resetOffsets();
      KafkaStreamLevelConsumerManager.releaseKafkaConsumer(_consumer);
      _consumer = null;
    }
  }
}
