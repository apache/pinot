package org.apache.pinot.spi.stream.v2;

public interface ConsumerV2 {
  FetchResult fetch(Checkpoint start, Checkpoint end, long timeout);
}

