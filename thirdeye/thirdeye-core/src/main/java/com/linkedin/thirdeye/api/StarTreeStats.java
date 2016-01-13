package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StarTreeStats {
  private final AtomicInteger nodeCount = new AtomicInteger(0);
  private final AtomicInteger leafCount = new AtomicInteger(0);
  private final AtomicInteger recordCount = new AtomicInteger(0);
  private final AtomicLong minTime = new AtomicLong(-1);
  private final AtomicLong maxTime = new AtomicLong(-1);

  public void countNode() {
    nodeCount.incrementAndGet();
  }

  public void countLeaf() {
    leafCount.incrementAndGet();
  }

  public void countRecords(int records) {
    recordCount.addAndGet(records);
  }

  public void updateMinTime(long time) {
    if (time >= 0 && (minTime.get() == -1 || time < minTime.get())) {
      minTime.set(time);
    }
  }

  public void updateMaxTime(long time) {
    if (time >= 0 && (maxTime.get() == -1 || time > maxTime.get())) {
      maxTime.set(time);
    }
  }

  @JsonProperty
  public int getNodeCount() {
    return nodeCount.get();
  }

  @JsonProperty
  public int getLeafCount() {
    return leafCount.get();
  }

  @JsonProperty
  public int getRecordCount() {
    return recordCount.get();
  }

  @JsonProperty
  public long getMinTime() {
    return minTime.get();
  }

  @JsonProperty
  public long getMaxTime() {
    return maxTime.get();
  }
}
