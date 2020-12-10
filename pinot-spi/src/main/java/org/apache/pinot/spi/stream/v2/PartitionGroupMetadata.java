package org.apache.pinot.spi.stream.v2;

public interface PartitionGroupMetadata {
  Checkpoint getStartCheckpoint(); // similar to getStartOffset

  Checkpoint getEndCheckpoint(); // similar to getEndOffset

  void setStartCheckpoint(Checkpoint startCheckpoint);

  void setEndCheckpoint(Checkpoint endCheckpoint);

  byte[] serialize();

  PartitionGroupMetadata deserialize(byte[] blob);
}

