package org.apache.pinot.plugin.stream.kinesis;

import java.util.List;
import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.RowMetadata;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import software.amazon.awssdk.services.kinesis.model.Record;


public class KinesisRecordsBatch implements MessageBatch<byte[]> {
  private List<Record> _recordList;

  public KinesisRecordsBatch(List<Record> recordList) {
    _recordList = recordList;
  }

  @Override
  public int getMessageCount() {
    return _recordList.size();
  }

  @Override
  public byte[] getMessageAtIndex(int index) {
    return _recordList.get(index).data().asByteArray();
  }

  @Override
  public int getMessageOffsetAtIndex(int index) {
    //TODO: Doesn't translate to offset. Needs to be replaced.
    return _recordList.get(index).hashCode();
  }

  @Override
  public int getMessageLengthAtIndex(int index) {
    return _recordList.get(index).data().asByteArray().length;
  }

  @Override
  public RowMetadata getMetadataAtIndex(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StreamPartitionMsgOffset getNextStreamParitionMsgOffsetAtIndex(int index) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getNextStreamMessageOffsetAtIndex(int index) {
    throw new UnsupportedOperationException();
  }
}
