package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StarTreeStats
{
  private final AtomicInteger nodeCount = new AtomicInteger(0);
  private final AtomicInteger leafCount = new AtomicInteger(0);
  private final AtomicInteger recordCount = new AtomicInteger(0);
  private final AtomicLong byteCount = new AtomicLong(0L);

  public void countNode()
  {
    nodeCount.incrementAndGet();
  }

  public void countLeaf()
  {
    leafCount.incrementAndGet();
  }

  public void countRecords(int records)
  {
    recordCount.addAndGet(records);
  }

  public void countBytes(long bytes)
  {
    byteCount.addAndGet(bytes);
  }

  @JsonProperty
  public AtomicInteger getNodeCount()
  {
    return nodeCount;
  }

  @JsonProperty
  public AtomicInteger getLeafCount()
  {
    return leafCount;
  }

  @JsonProperty
  public AtomicInteger getRecordCount()
  {
    return recordCount;
  }

  @JsonProperty
  public AtomicLong getByteCount()
  {
    return byteCount;
  }
}
