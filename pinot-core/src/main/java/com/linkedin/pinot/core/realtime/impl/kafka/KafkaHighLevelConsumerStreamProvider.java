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
package com.linkedin.pinot.core.realtime.impl.kafka;

import com.linkedin.pinot.common.metrics.ServerMeter;
import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.yammer.metrics.core.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;


/**
 *
 */
public class KafkaHighLevelConsumerStreamProvider implements StreamProvider {
  private static final Logger STATIC_LOGGER = LoggerFactory.getLogger(KafkaHighLevelConsumerStreamProvider.class);

  private KafkaHighLevelStreamProviderConfig streamProviderConfig;
  private KafkaMessageDecoder decoder;

  private ConsumerConnector consumer;
  private ConsumerIterator<byte[], byte[]> kafkaIterator;
  private long lastLogTime = 0;
  private long lastCount = 0;
  private ConsumerAndIterator consumerAndIterator;

  private Logger INSTANCE_LOGGER = STATIC_LOGGER;

  private ServerMetrics serverMetrics;
  private String tableAndStreamName;
  private long currentCount = 0L;

  private Meter tableAndStreamRowsConsumed = null;
  private Meter tableRowsConsumed = null;

  @Override
  public void init(StreamProviderConfig streamProviderConfig, String tableName, ServerMetrics serverMetrics)
      throws Exception {
    this.streamProviderConfig = (KafkaHighLevelStreamProviderConfig) streamProviderConfig;
    this.decoder = this.streamProviderConfig.getDecoder();
    tableAndStreamName = tableName + "-" + streamProviderConfig.getStreamName();
    INSTANCE_LOGGER = LoggerFactory.getLogger(
        KafkaHighLevelConsumerStreamProvider.class.getName() + "_" + tableName + "_" + streamProviderConfig
            .getStreamName());
    this.serverMetrics = serverMetrics;
  }

  @Override
  public void start() throws Exception {
    consumerAndIterator = KafkaConsumerManager.acquireConsumerAndIteratorForConfig(streamProviderConfig);
    kafkaIterator = consumerAndIterator.getIterator();
    consumer = consumerAndIterator.getConsumer();
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GenericRow next(GenericRow destination) {
    if (kafkaIterator.hasNext()) {
      try {
        destination = decoder.decode(kafkaIterator.next().message(), destination);
        tableAndStreamRowsConsumed = serverMetrics.addMeteredTableValue(tableAndStreamName, ServerMeter.REALTIME_ROWS_CONSUMED, 1L, tableAndStreamRowsConsumed);
        tableRowsConsumed = serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_ROWS_CONSUMED, 1L, tableRowsConsumed);
        ++currentCount;

        final long now = System.currentTimeMillis();
        // Log every minute or 100k events
        if (now - lastLogTime > 60000 || currentCount - lastCount >= 100000) {
          if (lastCount == 0) {
            INSTANCE_LOGGER.info("Consumed {} events from kafka stream {}", currentCount,
                this.streamProviderConfig.getStreamName());
          } else {
            INSTANCE_LOGGER.info("Consumed {} events from kafka stream {} (rate:{}/s)", currentCount - lastCount,
                this.streamProviderConfig.getStreamName(),
                (float) (currentCount - lastCount) * 1000 / (now - lastLogTime));
          }
          lastCount = currentCount;
          lastLogTime = now;
        }
        return destination;
      } catch (Exception e) {
        INSTANCE_LOGGER.warn("Caught exception while consuming events", e);
        serverMetrics.addMeteredTableValue(tableAndStreamName, ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_CONSUMPTION_EXCEPTIONS, 1L);
        throw e;
      }
    }
    return null;
  }

  @Override
  public GenericRow next(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long currentOffset() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void commit() {
    consumer.commitOffsets();
    serverMetrics.addMeteredTableValue(tableAndStreamName, ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
    serverMetrics.addMeteredGlobalValue(ServerMeter.REALTIME_OFFSET_COMMITS, 1L);
  }

  @Override
  public void commit(long offset) {
    throw new UnsupportedOperationException();
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
