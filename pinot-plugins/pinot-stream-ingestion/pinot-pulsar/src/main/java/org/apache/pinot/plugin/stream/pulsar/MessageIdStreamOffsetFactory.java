package org.apache.pinot.plugin.stream.pulsar;

import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffsetFactory;


public class MessageIdStreamOffsetFactory implements StreamPartitionMsgOffsetFactory {
  private StreamConfig _streamConfig;

  @Override
  public void init(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  @Override
  public StreamPartitionMsgOffset create(String offsetStr) {
    return new MessageIdStreamOffset(offsetStr);
  }

  @Override
  public StreamPartitionMsgOffset create(StreamPartitionMsgOffset other) {
    MessageIdStreamOffset messageIdStreamOffset = (MessageIdStreamOffset) other;
    return new MessageIdStreamOffset(messageIdStreamOffset.getMessageId());
  }
}
