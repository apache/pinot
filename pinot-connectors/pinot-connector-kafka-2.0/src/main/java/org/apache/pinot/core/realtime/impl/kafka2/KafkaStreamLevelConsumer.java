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

import com.yammer.metrics.core.Meter;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.metadata.instance.InstanceZKMetadata;
import org.apache.pinot.common.metrics.ServerMeter;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.realtime.stream.StreamConfig;
import org.apache.pinot.core.realtime.stream.StreamDecoderProvider;
import org.apache.pinot.core.realtime.stream.StreamLevelConsumer;
import org.apache.pinot.core.realtime.stream.StreamMessageDecoder;
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
  private KafkaHighLevelStreamConfig _kafkaHighLevelStreamConfig;

  private KafkaConsumer<Bytes, Bytes> consumer;
  private ConsumerRecords<Bytes, Bytes> consumerRecords;
  private Iterator<ConsumerRecord<Bytes, Bytes>> kafkaIterator;
  private Map<Integer, Long> consumerOffsets = new HashMap<>(); // tracking current consumed records offsets.

  private long lastLogTime = 0;
  private long lastCount = 0;
  private long currentCount = 0L;

  private ServerMetrics _serverMetrics;
  private Meter tableAndStreamRowsConsumed = null;
  private Meter tableRowsConsumed = null;

  public KafkaStreamLevelConsumer(String clientId, String tableName, StreamConfig streamConfig, Schema schema,
      InstanceZKMetadata instanceZKMetadata, ServerMetrics serverMetrics) {
    _clientId = clientId;
    _streamConfig = streamConfig;
    _kafkaHighLevelStreamConfig = new KafkaHighLevelStreamConfig(streamConfig, tableName, instanceZKMetadata);
    _serverMetrics = serverMetrics;

    _messageDecoder = StreamDecoderProvider.create(streamConfig, schema);

    _tableAndStreamName = tableName + "-" + streamConfig.getTopicName();
    INSTANCE_LOGGER = LoggerFactory
        .getLogger(KafkaStreamLevelConsumer.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
    INSTANCE_LOGGER.info("KafkaStreamLevelConsumer: streamConfig : {}", _streamConfig);
  }

  @Override
  public void start()
      throws Exception {
    consumer = KafkaStreamLevelConsumerManager.acquireKafkaConsumerForConfig(_kafkaHighLevelStreamConfig);
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
        tableAndStreamRowsConsumed = _serverMetrics
            .addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_ROWS_CONSUMED, 1L,
                tableAndStreamRowsConsumed);
        tableRowsConsumed =
            _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_ROWS_CONSUMED, 1L, tableRowsConsumed);

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
        _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
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
    _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
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
  public void shutdown()
      throws Exception {
    if (consumer != null) {
      // If offsets commit is not succeed, then reset the offsets here.
      resetOffsets();
      KafkaStreamLevelConsumerManager.releaseKafkaConsumer(consumer);
      consumer = null;
    }
  }
}
