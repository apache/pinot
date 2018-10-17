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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.linkedin.pinot.core.realtime.stream.MessageBatch;
import com.linkedin.pinot.core.realtime.stream.PartitionLevelConsumer;
import com.linkedin.pinot.core.realtime.stream.StreamConfig;
import java.io.IOException;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of PartitionLevelConsumer using Kafka's SimpleConsumer which ensures that we're connected to the appropriate broker for consumption.
 */
public class KafkaPartitionLevelConsumer extends KafkaConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPartitionLevelConsumer.class);

  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition, new KafkaSimpleConsumerFactoryImpl());
  }

  @VisibleForTesting
  public KafkaPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition,
      KafkaSimpleConsumerFactory kafkaSimpleConsumerFactory) {
    super(clientId, streamConfig, partition, kafkaSimpleConsumerFactory);
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
  @Override
  public synchronized MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis)
      throws java.util.concurrent.TimeoutException {
    // TODO Improve error handling

    final long connectEndTime = System.currentTimeMillis() + _connectTimeoutMillis;
    while (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER
        && System.currentTimeMillis() < connectEndTime) {
      _currentState.process();
    }
    if (_currentState.getStateValue() != KafkaConnectionHandler.ConsumerState.CONNECTED_TO_PARTITION_LEADER
        && connectEndTime <= System.currentTimeMillis()) {
      throw new java.util.concurrent.TimeoutException();
    }

    FetchResponse fetchResponse = _simpleConsumer.fetch(new FetchRequestBuilder().minBytes(100000)
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

  private Iterable<MessageAndOffset> buildOffsetFilteringIterable(final ByteBufferMessageSet messageAndOffsets,
      final long startOffset, final long endOffset) {
    return Iterables.filter(messageAndOffsets, input -> {
      // Filter messages that are either null or have an offset ∉ [startOffset; endOffset[
      if (input == null || input.offset() < startOffset || (endOffset <= input.offset() && endOffset != -1)) {
        return false;
      }

      // Check the message's checksum
      // TODO We might want to have better handling of this situation, maybe try to fetch the message again?
      if (!input.message().isValid()) {
        LOGGER.warn("Discarded message with invalid checksum in partition {} of topic {}", _partition, _topic);
        return false;
      }

      return true;
    });
  }

  @Override
  /**
   * Closes this consumer.
   */ public void close() throws IOException {
    super.close();
  }
}
