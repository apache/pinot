package com.linkedin.thirdeye.api;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StarTreeStats
{
  private final List<DimensionSpec> dimensions;
  private final List<MetricSpec> metrics;
  private final String timeColumnName;
  private final TimeUnit timeColumnAggregationUnit;
  private final int timeColumnAggregationSize;

  private final AtomicInteger nodeCount = new AtomicInteger(0);
  private final AtomicInteger leafCount = new AtomicInteger(0);
  private final AtomicInteger recordCount = new AtomicInteger(0);
  private final AtomicLong byteCount = new AtomicLong(0L);
  private final AtomicLong minTime = new AtomicLong(Long.MAX_VALUE);
  private final AtomicLong maxTime = new AtomicLong(0);

  public StarTreeStats(List<DimensionSpec> dimensions,
                       List<MetricSpec> metrics,
                       String timeColumnName,
                       int timeColumnAggregationSize,
                       TimeUnit timeColumnAggregationUnit)
  {
    this.dimensions = dimensions;
    this.metrics = metrics;
    this.timeColumnName = timeColumnName;
    this.timeColumnAggregationSize = timeColumnAggregationSize;
    this.timeColumnAggregationUnit = timeColumnAggregationUnit;
  }

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

  public void updateMinTime(long time)
  {
    if (time < minTime.get())
    {
      minTime.set(time);
    }
  }

  public void updateMaxTime(long time)
  {
    if (time > maxTime.get())
    {
      maxTime.set(time);
    }
  }

  @JsonProperty
  public int getNodeCount()
  {
    return nodeCount.get();
  }

  @JsonProperty
  public int getLeafCount()
  {
    return leafCount.get();
  }

  @JsonProperty
  public int getRecordCount()
  {
    return recordCount.get();
  }

  @JsonProperty
  public long getByteCount()
  {
    return byteCount.get();
  }

  @JsonProperty
  public long getMinTime()
  {
    return minTime.get();
  }

  @JsonProperty
  public long getMaxTime()
  {
    return maxTime.get();
  }

  @JsonProperty
  public List<DimensionSpec> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  public List<MetricSpec> getMetrics()
  {
    return metrics;
  }

  @JsonProperty
  public String getTimeColumnName()
  {
    return timeColumnName;
  }

  @JsonProperty
  public TimeUnit getTimeColumnAggregationUnit()
  {
    return timeColumnAggregationUnit;
  }

  @JsonProperty
  public int getTimeColumnAggregationSize()
  {
    return timeColumnAggregationSize;
  }
}
