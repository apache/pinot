package org.apache.pinot.core.realtime.impl.fakestream;

import org.apache.pinot.spi.stream.Checkpoint;
import org.apache.pinot.spi.stream.PartitionGroupMetadata;


public class FakePartitionGroupMetadata implements PartitionGroupMetadata {

  private final int _groupId;
  public FakePartitionGroupMetadata(int groupId) {
    _groupId = groupId;
  }

  @Override
  public int getGroupId() {
    return getGroupId();
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
