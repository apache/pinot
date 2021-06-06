package org.apache.pinot.plugin.stream.pulsar;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pulsar.client.api.MessageId;


public class MessageIdStreamOffset implements StreamPartitionMsgOffset {
  private MessageId _messageId;

  public MessageIdStreamOffset(MessageId messageId){
    _messageId = messageId;
  }

  public MessageIdStreamOffset(String messageId){
    try {
      _messageId = MessageId.fromByteArray(messageId.getBytes(StandardCharsets.UTF_8));
    }catch (IOException e){
      //TODO: handle invalid message id error
    }
  }

  public MessageId getMessageId() {
    return _messageId;
  }

  @Override
  public StreamPartitionMsgOffset fromString(String streamPartitionMsgOffsetStr) {
    return new MessageIdStreamOffset(streamPartitionMsgOffsetStr);
  }

  @Override
  public int compareTo(Object other) {
    MessageIdStreamOffset messageIdStreamOffset = (MessageIdStreamOffset) other;
   return _messageId.compareTo(messageIdStreamOffset.getMessageId());
  }

  @Override
  public String toString() {
    return _messageId.toString();
  }
}
