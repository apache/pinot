/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


/**
 *
 */
public class KafkaHighLevelConsumerStreamProvider implements StreamProvider {
  private static final Logger STATIC_LOGGER = LoggerFactory.getLogger(KafkaHighLevelConsumerStreamProvider.class);

  private static Counter globalKafkaEventsConsumedCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class, "kafkaEventsConsumedCount"));
  private static Counter globalKafkaEventsFailedCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class, "kafkaEventsFailedCount"));
  private static Counter globalKafkaEventsCommitCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class, "kafkaEventsCommitCount"));

  private KafkaHighLevelStreamProviderConfig streamProviderConfig;
  private KafkaMessageDecoder decoder;

  private ConsumerConfig kafkaConsumerConfig;
  private ConsumerConnector consumer;
  private KafkaStream<byte[], byte[]> kafkaStreams;
  private ConsumerIterator<byte[], byte[]> kafkaIterator;
  private long lastLogTime = 0;
  private long lastCount = 0;

  private Logger INSTANCE_LOGGER = STATIC_LOGGER;
  private Counter kafkaEventsConsumedCount = globalKafkaEventsConsumedCount;
  private Counter kafkaEventsFailedCount = globalKafkaEventsFailedCount;
  private Counter kafkaEventsCommitCount = globalKafkaEventsCommitCount;

  @Override
  public void init(StreamProviderConfig streamProviderConfig, String tableName) throws Exception {
    this.streamProviderConfig = (KafkaHighLevelStreamProviderConfig) streamProviderConfig;
    this.kafkaConsumerConfig = this.streamProviderConfig.getKafkaConsumerConfig();
    this.decoder = this.streamProviderConfig.getDecoder();
    kafkaEventsConsumedCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class,
            tableName + "-" + this.streamProviderConfig.getStreamName() + "-" + "kafkaEventsConsumedCount"));
    kafkaEventsFailedCount = Metrics.newCounter(
        new MetricName(KafkaHighLevelConsumerStreamProvider.class,
            tableName + "-" + this.streamProviderConfig.getStreamName() + "-" + "kafkaEventsFailedCount"));
    kafkaEventsCommitCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class,
            tableName + "-" + this.streamProviderConfig.getStreamName() + "-" + "kafkaEventsCommitCount"));
    INSTANCE_LOGGER = LoggerFactory.getLogger(
        KafkaHighLevelConsumerStreamProvider.class.getName() + "_" + tableName + "_" + streamProviderConfig
            .getStreamName());
  }

  @Override
  public void start() throws Exception {
    consumer = kafka.consumer.Consumer.createJavaConsumerConnector(this.kafkaConsumerConfig);
    Map<String, Integer> topicsMap = streamProviderConfig.getTopicMap(1);

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicsMap);
    this.kafkaStreams = consumerMap.get(streamProviderConfig.getTopicName()).get(0);
    kafkaIterator = kafkaStreams.iterator();
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GenericRow next() {
    if (kafkaIterator.hasNext()) {
      try {
        GenericRow row = decoder.decode(kafkaIterator.next().message());
        kafkaEventsConsumedCount.inc();
        globalKafkaEventsConsumedCount.inc();

        // TODO Remove this logging stuff
        final long now = System.currentTimeMillis();
        final long currentCount = kafkaEventsConsumedCount.count();
        // Log every minute or 100k events
        if (now - lastLogTime > 60000 || currentCount - lastCount >= 100000) {
          if (lastCount == 0) {
            INSTANCE_LOGGER.info("Consumed {} events from kafka", currentCount);
          } else {
            INSTANCE_LOGGER.info("Consumed {} events from kafka (rate:{}/s)", currentCount - lastCount,
                (float) (currentCount - lastCount) * 1000 / (now - lastLogTime));
          }
          lastCount = currentCount;
          lastLogTime = now;
        }
        return row;
      } catch (Exception e) {
        INSTANCE_LOGGER.warn("Caught exception while consuming events", e);
        kafkaEventsFailedCount.inc();
        globalKafkaEventsFailedCount.inc();
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
    kafkaEventsCommitCount.inc();
    globalKafkaEventsCommitCount.inc();
  }

  @Override
  public void commit(long offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() throws Exception {
    if (consumer != null) {
      consumer.shutdown();
    }
  }
}
