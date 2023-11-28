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
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PartitionGroupConsumer} implementation for the Pulsar stream
 */
public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler
    implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);
  private final Reader _reader;
  private boolean _enableKeyValueStitch;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    super(clientId, streamConfig);
    PulsarConfig config = new PulsarConfig(streamConfig, clientId);
    _reader =
        createReaderForPartition(config.getPulsarTopicName(), partitionGroupConsumptionStatus.getPartitionGroupId(),
            config.getInitialMessageId());
    LOGGER.info("Created pulsar reader with id {} for topic {} partition {}", _reader, _config.getPulsarTopicName(),
        partitionGroupConsumptionStatus.getPartitionGroupId());
    _enableKeyValueStitch = _config.getEnableKeyValueStitch();
  }

  /**
   * Fetch records from the Pulsar stream between the start and end StreamPartitionMsgOffset
   * Used {@link org.apache.pulsar.client.api.Reader} to read the messaged from pulsar partitioned topic
   * The reader seeks to the startMsgOffset and starts reading records in a loop until endMsgOffset or timeout is
   * reached.
   */
  @Override
  public PulsarMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset,
      StreamPartitionMsgOffset endMsgOffset, int timeoutMillis) {
    final MessageId startMessageId = ((MessageIdStreamOffset) startMsgOffset).getMessageId();
    final MessageId endMessageId =
        endMsgOffset == null ? MessageId.latest : ((MessageIdStreamOffset) endMsgOffset).getMessageId();

    final Collection<PulsarStreamMessage> messages = Collections.synchronizedList(new ArrayList<>());

    CompletableFuture<PulsarMessageBatch> pulsarResultFuture = fetchMessagesAsync(startMessageId, endMessageId, messages)
        .orTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
        .handle((v, t) -> {
          if (! (t instanceof TimeoutException)) {
            LOGGER.warn("Error while fetching records from Pulsar", t);
          }
          return new PulsarMessageBatch(buildOffsetFilteringIterable(messages, startMessageId, endMessageId),
              _enableKeyValueStitch);
        });

    try {
      return pulsarResultFuture.get();
    } catch (Exception e) {
      LOGGER.warn("Error while fetching records from Pulsar", e);
      return new PulsarMessageBatch(buildOffsetFilteringIterable(messages, startMessageId, endMessageId),
          _enableKeyValueStitch);
    }
  }

  public CompletableFuture<Void> fetchMessagesAsync(MessageId startMessageId, MessageId endMessageId,
      Collection<PulsarStreamMessage> messages) {
      CompletableFuture<Void> seekFut = _reader.seekAsync(startMessageId);
      return seekFut.thenCompose((v) -> fetchNextMessageAndAddToCollection(endMessageId, messages));
  }

  public CompletableFuture<Void> fetchNextMessageAndAddToCollection(MessageId endMessageId,
      Collection<PulsarStreamMessage> messages) {
    CompletableFuture<Boolean> hasMessagesFut = _reader.hasMessageAvailableAsync();
    CompletableFuture<Message<byte[]>> messageFut = hasMessagesFut.thenCompose(msgAvailable ->
        (msgAvailable)? _reader.readNextAsync() : CompletableFuture.completedFuture(null));
    CompletableFuture<Void> handleMessageFut = messageFut.thenCompose(messageOrNull ->
        readMessageAndFetchNextOrComplete(endMessageId, messages, messageOrNull));
    return handleMessageFut;
  }

  public CompletableFuture<Void> readMessageAndFetchNextOrComplete(MessageId endMessageId,
      Collection<PulsarStreamMessage> messages, Message<byte[]> messageOrNull) {
    if (messageOrNull == null) {
      return CompletableFuture.completedFuture(null);
    }
    if (endMessageId != null && messageOrNull.getMessageId().compareTo(endMessageId) > 0) {
      return CompletableFuture.completedFuture(null);
    }
    messages.add(PulsarUtils.buildPulsarStreamMessage(messageOrNull, _enableKeyValueStitch, _pulsarMetadataExtractor));
    return fetchNextMessageAndAddToCollection(endMessageId, messages);
  }

  private Iterable<PulsarStreamMessage> buildOffsetFilteringIterable(final Iterable<PulsarStreamMessage> messageAndOffsets,
      final MessageId startOffset, final MessageId endOffset) {
    return Iterables.filter(messageAndOffsets, input -> {
      // Filter messages that are either null or have an offset ∉ [startOffset, endOffset]
      return input != null && input.getValue() != null && (input.getMessageId().compareTo(startOffset) >= 0) && (
          (endOffset == null) || (input.getMessageId().compareTo(endOffset) < 0));
    });
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
    super.close();
  }

}
