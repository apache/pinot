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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pinot.spi.stream.BytesStreamMessage;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
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

  // TODO: Revisit the logic of using a separate executor to manage the request timeout. Currently it is not thread safe
  private final ExecutorService _executorService = Executors.newSingleThreadExecutor();

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partitionId) {
    super(clientId, streamConfig);
    try {
      _reader = createReaderForPartition(partitionId);
    } catch (Exception e) {
      throw new RuntimeException("Caught exception while creating Pulsar reader", e);
    }
    LOGGER.info("Created Pulsar reader with topic: {}, partition: {}, initial message id: {}",
        _config.getPulsarTopicName(), partitionId, _config.getInitialMessageId());
  }

  /**
   * Fetch records from the Pulsar stream between the start and end StreamPartitionMsgOffset
   * Used {@link org.apache.pulsar.client.api.Reader} to read the messaged from pulsar partitioned topic
   * The reader seeks to the startMsgOffset and starts reading records in a loop until endMsgOffset or timeout is
   * reached.
   */
  @Override
  public PulsarMessageBatch fetchMessages(StreamPartitionMsgOffset startMsgOffset, int timeoutMs) {
    MessageIdStreamOffset startOffset = (MessageIdStreamOffset) startMsgOffset;
    List<BytesStreamMessage> messages = new ArrayList<>();
    Future<PulsarMessageBatch> pulsarResultFuture = _executorService.submit(() -> fetchMessages(startOffset, messages));
    try {
      return pulsarResultFuture.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      // The fetchMessages has thrown an exception. Most common cause is the timeout.
      // We return the records fetched till now along with the next start offset.
      pulsarResultFuture.cancel(true);
    } catch (Exception e) {
      LOGGER.warn("Error while fetching records from Pulsar", e);
    }
    return buildPulsarMessageBatch(startOffset, messages);
  }

  private PulsarMessageBatch fetchMessages(MessageIdStreamOffset startOffset, List<BytesStreamMessage> messages) {
    try {
      MessageId startMessageId = startOffset.getMessageId();
      _reader.seek(startMessageId);
      while (_reader.hasMessageAvailable()) {
        Message<byte[]> message = _reader.readNext();
        messages.add(PulsarUtils.buildPulsarStreamMessage(message, _config));
        if (Thread.interrupted()) {
          break;
        }
      }
    } catch (PulsarClientException e) {
      LOGGER.warn("Error consuming records from Pulsar topic", e);
    }
    return buildPulsarMessageBatch(startOffset, messages);
  }

  private PulsarMessageBatch buildPulsarMessageBatch(MessageIdStreamOffset startOffset,
      List<BytesStreamMessage> messages) {
    MessageIdStreamOffset offsetOfNextBatch;
    if (messages.isEmpty()) {
      offsetOfNextBatch = startOffset;
    } else {
      StreamMessageMetadata lastMessageMetadata = messages.get(messages.size() - 1).getMetadata();
      assert lastMessageMetadata != null;
      offsetOfNextBatch = (MessageIdStreamOffset) lastMessageMetadata.getNextOffset();
    }
    return new PulsarMessageBatch(messages, offsetOfNextBatch, false);
  }

  @Override
  public void close()
      throws IOException {
    _reader.close();
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
