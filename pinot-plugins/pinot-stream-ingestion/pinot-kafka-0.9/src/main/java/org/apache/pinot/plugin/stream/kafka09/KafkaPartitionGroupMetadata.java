package org.apache.pinot.plugin.stream.kafka09;

import org.apache.pinot.spi.stream.Checkpoint;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;


public class KafkaPartitionGroupMetadata implements PartitionGroupMetadata {

  private final int _groupId;
  public KafkaPartitionGroupMetadata(int partitionId) {
    _groupId = partitionId;
  }

  @Override
  public int getGroupId() {
    return _groupId;
  }

  @Override
  public Checkpoint getStartCheckpoint() {
    return null;
  }

  @Override
  public Checkpoint getEndCheckpoint() {
    return null;
  }

  @Override
  public void setStartCheckpoint(Checkpoint startCheckpoint) {

  }

  @Override
  public void setEndCheckpoint(Checkpoint endCheckpoint) {

  }

  @Override
  public byte[] serialize() {
    return new byte[0];
  }

  @Override
  public PartitionGroupMetadata deserialize(byte[] blob) {
    return null;
  }
}
