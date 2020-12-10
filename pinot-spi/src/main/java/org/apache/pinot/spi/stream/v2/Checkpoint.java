package org.apache.pinot.spi.stream.v2;

public interface Checkpoint {
  byte[] serialize();
  Checkpoint deserialize(byte[] blob);
}
