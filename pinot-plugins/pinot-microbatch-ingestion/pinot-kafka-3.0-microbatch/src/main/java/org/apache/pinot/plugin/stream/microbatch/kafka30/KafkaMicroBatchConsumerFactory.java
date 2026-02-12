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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import org.apache.pinot.plugin.stream.kafka.KafkaConfigBackwardCompatibleUtils;
import org.apache.pinot.plugin.stream.kafka30.KafkaStreamMetadataProvider;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;
import org.apache.pinot.spi.utils.retry.RetryPolicy;


/**
 * Consumer factory for Kafka microbatch ingestion.
 *
 * <p>This factory creates consumers that expect Kafka messages in binary protocol format
 * (version byte + JSON payload) with references to batch files (URIs) or inline data.
 *
 * <p>Configuration example:
 * <pre>
 * {
 *   "streamType": "kafka",
 *   "stream.kafka.topic.name": "microbatch-topic",
 *   "stream.kafka.broker.list": "localhost:9092",
 *   "stream.kafka.consumer.factory.class.name":
 *     "org.apache.pinot.plugin.stream.microbatch.kafka30.KafkaMicroBatchConsumerFactory",
 *   "stream.microbatch.kafka.file.fetch.threads": "4"
 * }
 * </pre>
 *
 * <p>For file URIs in protocol messages, configure PinotFS:
 * <pre>
 * # For S3
 * pinot.fs.s3.class=org.apache.pinot.plugin.filesystem.S3PinotFS
 * pinot.fs.s3.region=us-west-2
 *
 * # For HDFS
 * pinot.fs.hdfs.class=org.apache.pinot.plugin.filesystem.HadoopPinotFS
 * </pre>
 *
 * @see MicroBatchConfigProperties for all available configuration options
 */
public class KafkaMicroBatchConsumerFactory extends StreamConsumerFactory {

  private int _numFileFetchThreads = MicroBatchConfigProperties.DEFAULT_FILE_FETCH_THREADS;

  @Override
  protected void init(StreamConfig streamConfig) {
    KafkaConfigBackwardCompatibleUtils.handleStreamConfig(streamConfig);
    super.init(streamConfig);

    // Parse parallel dataloaders config
    int fileFetchThreads = MicroBatchConfigProperties.getIntProperty(
        streamConfig,
        MicroBatchConfigProperties.FILE_FETCH_THREADS,
        MicroBatchConfigProperties.DEFAULT_FILE_FETCH_THREADS);

    if (fileFetchThreads < 1) {
      throw new IllegalArgumentException(
          MicroBatchConfigProperties.FILE_FETCH_THREADS + " must be >= 1, got: " + fileFetchThreads);
    }
    _numFileFetchThreads = fileFetchThreads;
  }

  @Override
  public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int partition) {
    return new MicroBatchStreamMetadataProvider(
        new KafkaStreamMetadataProvider(clientId, _streamConfig, partition));
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
    return new MicroBatchStreamMetadataProvider(
        new KafkaStreamMetadataProvider(clientId, _streamConfig));
  }

  @Override
  public StreamMetadataProvider createStreamMetadataProvider(
      String clientId, boolean concurrentAccessExpected) {
    if (concurrentAccessExpected) {
      return new SynchronizedKafkaStreamMetadataProvider(
          new KafkaStreamMetadataProvider(clientId, _streamConfig));
    } else {
      return createStreamMetadataProvider(clientId);
    }
  }

  @Override
  public StreamPartitionMsgOffsetFactory createStreamMsgOffsetFactory() {
    return new MicroBatchStreamPartitionMsgOffsetFactory();
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(
      String clientId, PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    return new KafkaPartitionLevelMicroBatchConsumer(
        clientId, _streamConfig, partitionGroupConsumptionStatus.getStreamPartitionGroupId(),
        _numFileFetchThreads);
  }

  @Override
  public PartitionGroupConsumer createPartitionGroupConsumer(
      String clientId,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus,
      RetryPolicy retryPolicy) {
    return new KafkaPartitionLevelMicroBatchConsumer(
        clientId, _streamConfig, partitionGroupConsumptionStatus.getStreamPartitionGroupId(),
        retryPolicy, _numFileFetchThreads);
  }
}
