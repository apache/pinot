package org.apache.pinot.spi.stream.v2;

import java.util.List;


public interface FetchResult<T> {
  Checkpoint getLastCheckpoint();
  List<T> getMessages();
}

