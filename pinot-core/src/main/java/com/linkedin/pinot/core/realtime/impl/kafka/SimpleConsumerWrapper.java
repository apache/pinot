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

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.core.realtime.stream.MessageBatch;
import com.linkedin.pinot.core.realtime.stream.PinotStreamConsumer;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper for Kafka's SimpleConsumer which ensures that we're connected to the appropriate broker for consumption.
 */
public class SimpleConsumerWrapper extends KafkaConnectionHandler implements PinotStreamConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerWrapper.class);

  public SimpleConsumerWrapper(KafkaSimpleConsumerFactory simpleConsumerFactory, String bootstrapNodes,
      String clientId, String topic, long connectTimeoutMillis) {
    super(simpleConsumerFactory, bootstrapNodes, clientId, topic, connectTimeoutMillis);
  }

  public SimpleConsumerWrapper(KafkaSimpleConsumerFactory simpleConsumerFactory, String bootstrapNodes,
      String clientId, String topic, int partition, long connectTimeoutMillis) {
    super(simpleConsumerFactory, bootstrapNodes, clientId, topic, partition, connectTimeoutMillis);
  }

  public synchronized int getPartitionCount(String topic, long timeoutMillis) {
    int unknownTopicReplyCount = 0;
    final int MAX_UNKNOWN_TOPIC_REPLY_COUNT = 10;
    int kafkaErrorCount = 0;
    final int MAX_KAFKA_ERROR_COUNT = 10;

    final long endTime = System.currentTimeMillis() + timeoutMillis;

    while(System.currentTimeMillis() < endTime) {
      // Try to get into a state where we're connected to Kafka
      while (!_currentState.isConnectedToKafkaBroker() && System.currentTimeMillis() < endTime) {
        _currentState.process();
      }

      if (endTime <= System.currentTimeMillis() && !_currentState.isConnectedToKafkaBroker()) {
        throw new TimeoutException("Failed to get the partition count for topic " + topic + " within " + timeoutMillis
            + " ms");
      }

      // Send the metadata request to Kafka
      TopicMetadataResponse topicMetadataResponse = null;
      try {
        topicMetadataResponse = _simpleConsumer.send(new TopicMetadataRequest(Collections.singletonList(topic)));
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
        throw new RuntimeException("Invalid topic name " + topic);
      } else if (errorCode == Errors.UNKNOWN_TOPIC_OR_PARTITION.code()) {
        if (MAX_UNKNOWN_TOPIC_REPLY_COUNT < unknownTopicReplyCount) {
          throw new RuntimeException("Topic " + topic + " does not exist");
        } else {
          // Kafka topic creation can sometimes take some time, so we'll retry after a little bit
          unknownTopicReplyCount++;
          Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
        }
      } else {
        // Retry after a short delay
        kafkaErrorCount++;

        if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
          throw exceptionForKafkaErrorCode(errorCode);
        }

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }

    throw new TimeoutException();
  }

  /**
   * Fetch messages and the per-partition high watermark from Kafka between the specified offsets.
   *
   * @param startOffset The offset of the first message desired, inclusive
   * @param endOffset The offset of the last message desired, exclusive, or {@link Long#MAX_VALUE} for no end offset.
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An iterable containing messages fetched from Kafka and their offsets, as well as the high watermark for
   * this partition.
   */
  public synchronized MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis) throws java.util.concurrent.TimeoutException {
    Preconditions.checkState(_isPartitionMetadata, "Cannot fetch messages from a metadata-only SimpleConsumerWrapper");
    // Ensure that we're connected to the leader
    // TODO Improve error handling

    final long connectEndTime = System.currentTimeMillis() + _connectTimeoutMillis;
    while(_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER &&
        System.currentTimeMillis() < connectEndTime) {
      _currentState.process();
    }
    if (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER &&
        connectEndTime <= System.currentTimeMillis()) {
      throw new java.util.concurrent.TimeoutException();
    }

    FetchResponse fetchResponse = _simpleConsumer.fetch(new FetchRequestBuilder()
        .minBytes(100000)
        .maxWait(timeoutMillis)
        .addFetch(_topic, _partition, startOffset, 500000)
        .build());

    if (!fetchResponse.hasError()) {
      final Iterable<MessageAndOffset> messageAndOffsetIterable =
          buildOffsetFilteringIterable(fetchResponse.messageSet(_topic, _partition), startOffset, endOffset);

      // TODO: Instantiate with factory
      return new SimpleConsumerMessageBatch(messageAndOffsetIterable);
    } else {
      throw exceptionForKafkaErrorCode(fetchResponse.errorCode(_topic, _partition));
    }
  }

  /**
   * Fetches the numeric Kafka offset for this partition for a symbolic name ("largest" or "smallest").
   *
   * @param requestedOffset Either "largest" or "smallest"
   * @param timeoutMillis Timeout in milliseconds
   * @throws java.util.concurrent.TimeoutException If the operation could not be completed within {@code timeoutMillis}
   * milliseconds
   * @return An offset
   */
  public synchronized long fetchPartitionOffset(String requestedOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    Preconditions.checkNotNull(requestedOffset);

    final long offsetRequestTime;
    if (requestedOffset.equalsIgnoreCase("largest")) {
      offsetRequestTime = kafka.api.OffsetRequest.LatestTime();
    } else if (requestedOffset.equalsIgnoreCase("smallest")) {
      offsetRequestTime = kafka.api.OffsetRequest.EarliestTime();
    } else if (requestedOffset.equalsIgnoreCase("testDummy")) {
      return -1L;
    } else {
      throw new IllegalArgumentException("Unknown initial offset value " + requestedOffset);
    }

    int kafkaErrorCount = 0;
    final int MAX_KAFKA_ERROR_COUNT = 10;

    final long endTime = System.currentTimeMillis() + timeoutMillis;

    while(System.currentTimeMillis() < endTime) {
      // Try to get into a state where we're connected to Kafka
      while (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER &&
          System.currentTimeMillis() < endTime) {
        _currentState.process();
      }

      if (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER &&
          endTime <= System.currentTimeMillis()) {
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
        return offset;
      } else if (errorCode == Errors.LEADER_NOT_AVAILABLE.code()) {
        // If there is no leader, it'll take some time for a new leader to be elected, wait 100 ms before retrying
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } else {
        // Retry after a short delay
        kafkaErrorCount++;

        if (MAX_KAFKA_ERROR_COUNT < kafkaErrorCount) {
          throw exceptionForKafkaErrorCode(errorCode);
        }

        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    }

    throw new TimeoutException();
  }

  private Iterable<MessageAndOffset> buildOffsetFilteringIterable(final ByteBufferMessageSet messageAndOffsets, final long startOffset, final long endOffset) {
    return Iterables.filter(messageAndOffsets, new Predicate<MessageAndOffset>() {
      @Override
      public boolean apply(@Nullable MessageAndOffset input) {
        // Filter messages that are either null or have an offset ∉ [startOffset; endOffset[
        if(input == null || input.offset() < startOffset || (endOffset <= input.offset() && endOffset != -1)) {
          return false;
        }

        // Check the message's checksum
        // TODO We might want to have better handling of this situation, maybe try to fetch the message again?
        if(!input.message().isValid()) {
          LOGGER.warn("Discarded message with invalid checksum in partition {} of topic {}", _partition, _topic);
          return false;
        }

        return true;
      }
    });
  }

  @Override
  /**
   * Closes this consumer.
   */
  public void close() throws IOException {
    super.close();
  }
}
