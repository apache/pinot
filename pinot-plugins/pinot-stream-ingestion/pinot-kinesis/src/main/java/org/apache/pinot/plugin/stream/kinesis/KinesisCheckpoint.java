package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.v2.Checkpoint;


public class KinesisCheckpoint implements Checkpoint {
  String _shardId;
  String _sequenceNumber;

  public KinesisCheckpoint(String shardId, String sequenceNumber){
    _shardId = shardId;
    _sequenceNumber = sequenceNumber;
  }

  public String getSequenceNumber() {
    return _sequenceNumber;
  }

  public String getShardId() {
    return _shardId;
  }

  public void setShardId(String shardId) {
    _shardId = shardId;
  }

  @Override
  public byte[] serialize() {
    return _sequenceNumber.getBytes();
  }

  @Override
  public Checkpoint deserialize(byte[] blob) {
    //TODO: Implement SerDe
    return new KinesisCheckpoint("", new String(blob));
  }

}
