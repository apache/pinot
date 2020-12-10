package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.v2.Checkpoint;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;


public class KinesisCheckpoint implements Checkpoint {
  String _shardIterator;

  public KinesisCheckpoint(String shardIterator){
    _shardIterator = shardIterator;
  }

  public String getShardIterator() {
    return _shardIterator;
  }

  @Override
  public byte[] serialize() {
    return _shardIterator.getBytes();
  }

  @Override
  public Checkpoint deserialize(byte[] blob) {
    return new KinesisCheckpoint(new String(blob));
  }

}
