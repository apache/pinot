package org.apache.pinot.spi.stream;

public class ConsumerPartitionStatus {
  private final StreamPartitionMsgOffset _msgOffset;

  public ConsumerPartitionStatus(StreamPartitionMsgOffset msgOffset) {
    _msgOffset = msgOffset;
  }

  public StreamPartitionMsgOffset getMsgOffset() {
    return _msgOffset;
  }
}
