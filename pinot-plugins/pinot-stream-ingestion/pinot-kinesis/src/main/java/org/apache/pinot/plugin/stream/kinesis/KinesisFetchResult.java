package org.apache.pinot.plugin.stream.kinesis;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.FetchResult;
import software.amazon.awssdk.services.kinesis.model.Record;


public class KinesisFetchResult implements FetchResult<Record> {
  private final String _nextShardIterator;
  private final List<Record> _recordList;

  public KinesisFetchResult(String nextShardIterator, List<Record> recordList){
     _nextShardIterator = nextShardIterator;
     _recordList = recordList;
  }

  @Override
  public Checkpoint getLastCheckpoint() {
    return new KinesisCheckpoint(_nextShardIterator);
  }

  @Override
  public List<Record> getMessages() {
    return _recordList;
  }
}
