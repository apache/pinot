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
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PulsarPartitionLevelConsumer extends PulsarPartitionLevelConnectionHandler implements PartitionLevelConsumer {
  private static final Logger LOGGER = LoggerFactory.getLogger(PulsarPartitionLevelConsumer.class);
  private final ExecutorService _executorService;

  public PulsarPartitionLevelConsumer(String clientId, StreamConfig streamConfig, int partition) {
    super(clientId, streamConfig, partition);
    _executorService = Executors.newSingleThreadExecutor();
  }

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
    } catch (Exception e) {
      return new PulsarMessageBatch(buildOffsetFilteringIterable(messagesList, startMessageId, endMessageId));
    }
  }

  public PulsarMessageBatch fetchMessages(MessageId startMessageId, MessageId endMessageId,
      List<Message<byte[]>> messagesList)
      throws TimeoutException {
    //TODO: return n-1 records instead of n and use offset of nth record as next starting point
    try {
      _reader.seek(startMessageId);

      while (_reader.hasMessageAvailable()) {
        Message<byte[]> nextMessage = _reader.readNext();
        messagesList.add(nextMessage);
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
  public MessageBatch fetchMessages(long startOffset, long endOffset, int timeoutMillis) {
    throw new UnsupportedOperationException("Pulsar does not support long offsets");
  }

  @Override
  public void close()
      throws IOException {
    super.close();
  }
}
