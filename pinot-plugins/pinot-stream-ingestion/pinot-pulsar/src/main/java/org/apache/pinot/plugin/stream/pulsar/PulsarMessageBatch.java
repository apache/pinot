package org.apache.pinot.plugin.stream.pulsar;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.internal.DefaultImplementation;


public class PulsarMessageBatch implements MessageBatch<byte[]> {

  private List<Message<byte[]>> messageList = new ArrayList<>();

  public PulsarMessageBatch(Iterable<Message<byte[]>> iterable) {
    iterable.forEach(messageList::add);
  }

  @Override
  public int getMessageCount() {
    return messageList.size();
  }

  @Override
  public byte[] getMessageAtIndex(int index) {
    return messageList.get(index).getData();
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    return ByteBuffer.wrap(messageList.get(index).getData()).arrayOffset();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return messageList.get(index).getData().length;
  }

  @Override
  public StreamPartitionMsgOffset getNextStreamParitionMsgOffsetAtIndex(int index) {
    MessageIdImpl currentMessageId = MessageIdImpl.convertToMessageIdImpl(messageList.get(index).getMessageId());
    MessageId nextMessageId = DefaultImplementation
        .newMessageId(currentMessageId.getLedgerId(), currentMessageId.getEntryId() + 1,
            currentMessageId.getPartitionIndex());
    return new MessageIdStreamOffset(nextMessageId);
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException("Pulsar does not support long stream offsets");
  }
}
