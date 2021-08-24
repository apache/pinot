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
package org.apache.pinot.plugin.stream.kafka09;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.pinot.spi.stream.LongMsgOffset;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMetadataProvider;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of a stream metadata provider for a kafka stream using kafka'a simple consumer
 */
public class KafkaStreamMetadataProvider extends KafkaConnectionHandler implements StreamMetadataProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamMetadataProvider.class);

  /**
   * Create a partition specific metadata provider
   * @param streamConfig
   * @param partition
   */
  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition, new KafkaSimpleConsumerFactoryImpl());
  }

  /**
   * Create a stream specific metadata provider
   * @param streamConfig
   */
  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig) {
    super(clientId, streamConfig, new KafkaSimpleConsumerFactoryImpl());
  }

  @VisibleForTesting
  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig, int partition,
      KafkaSimpleConsumerFactory kafkaSimpleConsumerFactory) {
    super(clientId, streamConfig, partition, kafkaSimpleConsumerFactory);
  }

  @VisibleForTesting
  public KafkaStreamMetadataProvider(String clientId, StreamConfig streamConfig,
      KafkaSimpleConsumerFactory kafkaSimpleConsumerFactory) {
    super(clientId, streamConfig, kafkaSimpleConsumerFactory);
  }

  /**
   * Fetches the number of partitions for this kafka stream
   * @param timeoutMillis
   * @return
   */
  @Override
  public synchronized int fetchPartitionCount(long timeoutMillis) {
    int unknownTopicReplyCount = 0;
    final int maxUnknownTopicReplyCount = 10;
    int kafkaErrorCount = 0;
    final int maxKafkaErrorCount = 10;

    final long endTime = System.currentTimeMillis() + timeoutMillis;

    while (System.currentTimeMillis() < endTime) {
      // Try to get into a state where we're connected to Kafka
      while (!_currentState.isConnectedToKafkaBroker() && System.currentTimeMillis() < endTime) {
        _currentState.process();
      }

      if (endTime <= System.currentTimeMillis() && !_currentState.isConnectedToKafkaBroker()) {
        throw new TimeoutException(
            "Failed to get the partition count for topic " + _topic + " within " + timeoutMillis + " ms");
      }

      // Send the metadata request to Kafka
      TopicMetadataResponse topicMetadataResponse = null;
      try {
        topicMetadataResponse = _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(_topic)));
      } catch (Exception e) {
        _currentState.handleConsumerException(e);
        continue;
      }

      final TopicMetadata topicMetadata = topicMetadataResponse.topicsMetadata().get(0);
      final short errorCode = topicMetadata.errorCode();

      if (errorCode == Errors.NONE.code()) {
        return topicMetadata.partitionsMetadata().size();
      } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
        // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } else if (errorCode == Errors.INVALID_TOPIC_EXCEPTION.code()) {
        throw new RuntimeException("Invalid topic name " + _topic);
      } else if (errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
        if (maxUnknownTopicReplyCount < unknownTopicReplyCount) {
          throw new RuntimeException("Topic " + _topic + " does not exist");
        } else {
          // Kafka topic creation can sometimes take some time, so we'll retry after a little bit
          unknownTopicReplyCount++;
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      } else {
        // Retry after a short delay
        kafkaErrorCount++;

        if (maxKafkaErrorCount < kafkaErrorCount) {
          throw exceptionForKafkaErrorCode(errorCode);
        }

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }

    throw new TimeoutException();
  }

  public synchronized long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    throw new UnsupportedOperationException("The use of this method s not supported");
  }

  /**
   * Fetches the numeric Kafka offset for this partition for a symbolic name ("largest" or "smallest").
   *
   * @param offsetCriteria
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An offset
   */
  @Override
  public synchronized StreamPartitionMsgOffset fetchStreamPartitionOffset(@Nonnull OffsetCriteria offsetCriteria,
      long timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    Preconditions.checkState(_isPartitionProvided,
        "Cannot fetch partition offset. StreamMetadataProvider created without partition information");
    Preconditions.checkNotNull(offsetCriteria);

    final long offsetRequestTime;
    if (offsetCriteria.isLargest()) {
      offsetRequestTime = kafka.api.OffsetRequest.LatestTime();
    } else if (offsetCriteria.isSmallest()) {
      offsetRequestTime = kafka.api.OffsetRequest.EarliestTime();
    } else {
      throw new IllegalArgumentException("Unknown initial offset value " + offsetCriteria.toString());
    }

    int kafkaErrorCount = 0;
    final int maxKafkaErrorCount = 10;

    final long endTime = System.currentTimeMillis() + timeoutMillis;

    while (System.currentTimeMillis() < endTime) {
      // Try to get into a state where we're connected to Kafka
      while (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER
          && System.currentTimeMillis() < endTime) {
        _currentState.process();
      }

      if (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER
          && endTime <= System.currentTimeMillis()) {
        throw new TimeoutException();
      }

      // Send the offset request to Kafka
      OffsetRequest request = new OffsetRequest(Collections.singletonMap(new TopicAndPartition(_topic, _partition),
          new PartitionOffsetRequestInfo(offsetRequestTime, 1)), kafka.api.OffsetRequest.CurrentVersion(), _clientId);
      OffsetResponse offsetResponse;
      try {
        offsetResponse = _simpleConsumer.getOffsetsBefore(request);
      } catch (Exception e) {
        _currentState.handleConsumerException(e);
        continue;
      }

      final short errorCode = offsetResponse.errorCode(_topic, _partition);

      if (errorCode == Errors.NONE.code()) {
        long offset = offsetResponse.offsets(_topic, _partition)[0];
        if (offset == 0L) {
          LOGGER.warn("Fetched offset of 0 for topic {} and partition {}, is this a newly created topic?", _topic,
              _partition);
        }
        return new LongMsgOffset(offset);
      } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
        // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } else {
        // Retry after a short delay
        kafkaErrorCount++;

        if (maxKafkaErrorCount < kafkaErrorCount) {
          throw exceptionForKafkaErrorCode(errorCode);
        }

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }

    throw new TimeoutException();
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
