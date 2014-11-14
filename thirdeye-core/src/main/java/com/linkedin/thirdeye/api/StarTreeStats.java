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
  private final AtomicLong minTime = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong maxTime = new AtomicLong(0);

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

  public void updateMinTime(Long time)
  {
    if (time < minTime.get())
    {
      minTime.set(time);
    }
  }

  public void updateMaxTime(Long time)
  {
    if (time > maxTime.get())
    {
      maxTime.set(time);
    }
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

  @JsonProperty
  public Long getMinTime()
  {
    return minTime.get();
  }

  @JsonProperty
  public Long getMaxTime()
  {
    return maxTime.get();
  }
}
