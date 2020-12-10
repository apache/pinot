package org.apache.pinot.plugin.stream.kinesis;

import java.util.List;
import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.FetchResult;
import software.amazon.awssdk.services.kinesis.model.Record;


public class KinesisFetchResult implements FetchResult {
  private String _nextShardIterator;

  public KinesisFetchResult(String nextShardIterator, List<Record> recordList){
     _nextShardIterator = nextShardIterator;
  }

  @Override
  public Checkpoint getLastCheckpoint() {
    return new KinesisCheckpoint(_nextShardIterator);
  }

  @Override
  public byte[] getMessages() {
    return new byte[0];
  }
}
