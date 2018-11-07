/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.metadata.instance.InstanceZKMetadata;
import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import com.linkedin.pinot.core.realtime.stream.StreamDecoderProvider;
import com.linkedin.pinot.core.realtime.stream.StreamLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamMessageDecoder;
import com.yammer.metrics.core.Meter;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
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

  private ConsumerConnector consumer;
  private ConsumerIterator<byte[], byte[]> kafkaIterator;
  private ConsumerAndIterator consumerAndIterator;
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
    INSTANCE_LOGGER = LoggerFactory.getLogger(
        KafkaStreamLevelConsumer.class.getName() + "_" + tableName + "_" + streamConfig.getTopicName());
  }



  @Override
  public void start() throws Exception {
    consumerAndIterator = KafkaConsumerManager.acquireConsumerAndIteratorForConfig(_kafkaHighLevelStreamConfig);
    kafkaIterator = consumerAndIterator.getIterator();
    consumer = consumerAndIterator.getConsumer();
  }

  @Override
  public GenericRow next(GenericRow destination) {
    if (kafkaIterator.hasNext()) {
      try {
        destination = _messageDecoder.decode(kafkaIterator.next().message(), destination);
        tableAndStreamRowsConsumed =
            _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_ROWS_CONSUMED, 1L,
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
                _streamConfig.getTopicName(),
                (float) (currentCount - lastCount) * 1000 / (now - lastLogTime));
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

  @Override
  public void commit() {
    consumer.commitOffsets();
    _serverMetrics.addMeteredTableValue(_tableAndStreamName, ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
    _serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
  }

  @Override
  public void shutdown() throws Exception {
    if (consumerAndIterator != null) {
      kafkaIterator = null;
      consumer = null;

      KafkaConsumerManager.releaseConsumerAndIterator(consumerAndIterator);
      consumerAndIterator = null;
    }
  }
}
