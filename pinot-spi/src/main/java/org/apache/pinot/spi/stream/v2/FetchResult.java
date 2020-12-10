package org.apache.pinot.spi.stream.v2;

public interface FetchResult {
  Checkpoint getLastCheckpoint();
  byte[] getMessages();
}

