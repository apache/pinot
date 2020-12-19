package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.v2.Checkpoint;


public class KinesisCheckpoint implements Checkpoint {
  String _sequenceNumber;

  public KinesisCheckpoint(String sequenceNumber) {
    _sequenceNumber = sequenceNumber;
  }

  public String getSequenceNumber() {
    return _sequenceNumber;
  }

  @Override
  public byte[] serialize() {
    return _sequenceNumber.getBytes();
  }

  @Override
  public Checkpoint deserialize(byte[] blob) {
    //TODO: Implement SerDe
    return new KinesisCheckpoint(new String(blob));
  }
}
