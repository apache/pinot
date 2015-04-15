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

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class KafkaHighLevelConsumerStreamProvider implements StreamProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaHighLevelConsumerStreamProvider.class);

  private static Counter kafkaEventsConsumedCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class, "kafkaEventsConsumedCount"));
  private static Counter kafkaEventsFailedCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class, "kafkaEventsFailedCount"));
  private static Counter kafkaEventsCommitCount = Metrics.newCounter(new MetricName(KafkaHighLevelConsumerStreamProvider.class, "kafkaEventsCommitCount"));

  private KafkaHighLevelStreamProviderConfig streamProviderConfig;
  private KafkaMessageDecoder decoder;

  private ConsumerConfig kafkaConsumerConfig;
  private ConsumerConnector consumer;
  private KafkaStream<byte[], byte[]> kafkaStreams;
  private ConsumerIterator<byte[], byte[]> kafkaIterator;

  @Override
  public void init(StreamProviderConfig streamProviderConfig) throws Exception {
    this.streamProviderConfig = (KafkaHighLevelStreamProviderConfig) streamProviderConfig;
    this.kafkaConsumerConfig = this.streamProviderConfig.getKafkaConsumerConfig();
    this.decoder = this.streamProviderConfig.getDecoder();
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
        return row;
      } catch (Exception e) {
        LOGGER.warn("Caught exception while consuming events", e);
        kafkaEventsFailedCount.inc();
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
