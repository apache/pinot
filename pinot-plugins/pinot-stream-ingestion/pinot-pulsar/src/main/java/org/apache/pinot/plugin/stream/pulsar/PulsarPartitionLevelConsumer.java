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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link PartitionGroupConsumer} implementation for the Pulsar stream
 */
public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler implements PartitionGroupConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);
  private final ExecutorService _executorService;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig,
      PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
    super(clientId, streamConfig, partitionGroupConsumptionStatus.getPartitionGroupId());
    _executorService = Executors.newSingleThreadExecutor();
  }

  /**
   * Fetch records from the Pulsar stream between the start and end KinesisCheckpoint
   * Used {@link org.apache.pulsar.client.api.Reader} to read the messaged from pulsar partitioned topic
   * The reader seeks to the startMsgOffset and starts reading records in a loop until endMsgOffset or timeout is reached.
   */
  @Override
  public MessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, StreamPartitionMsgOffset endMsgOffset,
      int timeoutMillis) {
    final MessageId startMessageId = ((MessageIdStreamOffset) startMsgOffset).getMessageId();
    final MessageId endMessageId =
        endMsgOffset == null ? MessageId.latest : ((MessageIdStreamOffset) endMsgOffset).getMessageId();

    List<Message<byte[]>> messagesList = new ArrayList<>();
    Future<PulsarMessageBatch> pulsarResultFuture =
        _executorService.submit(() -> fetchMessages(startMessageId, endMessageId, messagesList));

    try {
      return pulsarResultFuture.get(timeoutMillis, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // The fetchMessages has thrown an exception. Most common cause is the timeout.
      // We return the records fetched till now along with the next start offset.
      pulsarResultFuture.cancel(true);
      return new PulsarMessageBatch(buildOffsetFilteringIterable(messagesList, startMessageId, endMessageId));
    } catch (Exception e) {
      LOGGER.warn("Error while fetching records from Pulsar", e);
      return new PulsarMessageBatch(buildOffsetFilteringIterable(messagesList, startMessageId, endMessageId));
    }
  }

  public PulsarMessageBatch fetchMessages(MessageId startMessageId, MessageId endMessageId,
      List<Message<byte[]>> messagesList) {
    try {
      _reader.seek(startMessageId);

      while (_reader.hasMessageAvailable()) {
        Message<byte[]> nextMessage = _reader.readNext();

        if (endMessageId != null) {
          if (nextMessage.getMessageId().compareTo(endMessageId) > 0) {
            break;
          }
        }
        messagesList.add(nextMessage);

        if (Thread.interrupted()) {
          break;
        }
      }

      return new PulsarMessageBatch(buildOffsetFilteringIterable(messagesList, startMessageId, endMessageId));
    } catch (PulsarClientException e) {
      LOGGER.warn("Error consuming records from Pulsar topic", e);
      return new PulsarMessageBatch(buildOffsetFilteringIterable(messagesList, startMessageId, endMessageId));
    }
  }

  private Iterable<Message<byte[]>> buildOffsetFilteringIterable(final List<Message<byte[]>> messageAndOffsets,
      final MessageId startOffset, final MessageId endOffset) {
    return Iterables.filter(messageAndOffsets, input -> {
      // Filter messages that are either null or have an offset ∉ [startOffset, endOffset]
      return input != null && input.getData() != null && (input.getMessageId().compareTo(startOffset) >= 0) && (
          (endOffset == null) || (input.getMessageId().compareTo(endOffset) < 0));
    });
  }

  @Override
  public void close()
      throws IOException {
    super.close();
    shutdownAndAwaitTermination();
  }

  void shutdownAndAwaitTermination() {
    _executorService.shutdown();
    try {
      if (!_executorService.awaitTermination(60, TimeUnit.SECONDS)) {
        _executorService.shutdownNow();
      }
    } catch (InterruptedException ie) {
      _executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
