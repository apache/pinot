package com.linkedin.thirdeye.impl.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.impl.StarTreeImpl;

public class MetricStoreImmutableImpl implements MetricStore, MetricStoreListener
{
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricStoreImmutableImpl.class);

  private static final TimeRange EMPTY_TIME_RANGE = new TimeRange(-1L, -1L);

  private final StarTreeConfig config;
  private final MetricSchema metricSchema;

  private ConcurrentMap<TimeRange, List<ByteBuffer>> buffers;
  private final AtomicReference<TimeRange> minTime;
  private final AtomicReference<TimeRange> maxTime;
  private ConcurrentMap<TimeRange, Integer> timeRangeCount;

  private int sizePerEntry;
  private boolean useBinarySearch = false;;

  public MetricStoreImmutableImpl(StarTreeConfig config,
      ConcurrentMap<TimeRange, List<ByteBuffer>> buffers)
  {
    this.config = config;
    this.buffers = buffers;
    this.metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    this.minTime = new AtomicReference<TimeRange>(EMPTY_TIME_RANGE);
    this.maxTime = new AtomicReference<TimeRange>(EMPTY_TIME_RANGE);
    this.timeRangeCount = new ConcurrentHashMap<>();

    if (!buffers.isEmpty())
    {
      minTime.set(Collections.min(buffers.keySet()));
      maxTime.set(Collections.max(buffers.keySet()));
    }
    // bytes required to store one time value (long and all the metrics)
    sizePerEntry = (Long.SIZE / 8 + metricSchema.getRowSizeInBytes());
  }

  @Override
  public void update(int id, MetricTimeSeries timeSeries)
  {
    throw new UnsupportedOperationException("update not supported on immutable store");
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException("clear not supported on immutable store");
  }

  @Override
  public MetricTimeSeries getTimeSeries(List<Integer> logicalOffsets, TimeRange timeRange)
  {
    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

    Set<Long> times = getTimes(timeRange);
    Collections.sort(logicalOffsets);
    for (Map.Entry<TimeRange, List<ByteBuffer>> entry : buffers.entrySet())
    {
      TimeRange bufferTimeRange = entry.getKey();
      if (bufferTimeRange.getStart() >= 0
          && (timeRange == null || !bufferTimeRange.isDisjoint(timeRange)))
      {
        for (ByteBuffer originalBuffer : entry.getValue())
        {
          ByteBuffer buffer = originalBuffer.duplicate();
          computeAggregateTimeSeries(logicalOffsets, timeRange, timeSeries, times, buffer);
        }
      }
    }

    return timeSeries;
  }

  private void computeAggregateTimeSeries(List<Integer> logicalOffsets, TimeRange timeRange,
      MetricTimeSeries timeSeries, Set<Long> times, ByteBuffer buffer)
  {
    buffer.rewind();
    for (Integer logicalOffset : logicalOffsets)
    {
      buffer.position(logicalOffset * (4 + 4));
      int startOffset = buffer.getInt();
      int length = buffer.getInt();
      if (length == 0)
      {
        continue;
      }
      buffer.position(startOffset);
      long minTime = buffer.getLong();
      int middle = 0;
      long timeRangeStartValue = timeRange.getStart();
      if (useBinarySearch)
      {
        middle = findStartIndex(buffer, startOffset, length, minTime, timeRangeStartValue);
      }
      if (middle == -1)
      {
        continue;
      }
      for (int i = middle; i < length; i++)
      {
        buffer.position(startOffset + (i * sizePerEntry));
        Long time = buffer.getLong();
        if (times.contains(time))
        {
          for (MetricSpec metricSpec : config.getMetrics())
          {
            Number value = NumberUtils.readFromBuffer(buffer, metricSpec.getType());
            timeSeries.increment(time, metricSpec.getName(), value);
          }
        }
        // since the times in buffer are sorted, if the time is greater the end of TimeRange
        // break
        if (time >= timeRange.getEnd())
        {
          break;
        }
      }
    }
  }

  private int findStartIndex(ByteBuffer buffer, int startOffset, int length, long minTime,
      long timeRangeStartValue)
  {
    int middle = -1;
    if (minTime >= timeRangeStartValue)
    {
      middle = 0;
    } else
    {
      // do a binary search to find the start Index
      int low = 0;
      int high = length - 1;
      boolean found = false;
      while (low <= high)
      {
        middle = (low + high) / 2;
        buffer.position(startOffset + (middle * sizePerEntry));
        final long midValue = buffer.getLong();
        if (timeRangeStartValue > midValue)
        {
          low = middle + 1;
        } else if (timeRangeStartValue < midValue)
        {
          high = middle - 1;
        } else
        {
          found = true;
          break;
        }
      }
    }
    return middle;
  }

  @Override
  public Long getMinTime()
  {
    return minTime.get().getStart();
  }

  @Override
  public Long getMaxTime()
  {
    return maxTime.get().getEnd();
  }

  @Override
  public Map<TimeRange, Integer> getTimeRangeCount()
  {

    for (Map.Entry<TimeRange, List<ByteBuffer>> entry : buffers.entrySet())
    {
      timeRangeCount.put(entry.getKey(), entry.getValue().size());
    }
    return timeRangeCount;
  }

  @Override
  public void notifyDelete(TimeRange timeRange)
  {
    Set<TimeRange> timeRanges = new HashSet<TimeRange>(buffers.keySet());
    timeRanges.remove(timeRange);
    this.minTime.set(timeRanges.isEmpty() ? EMPTY_TIME_RANGE : Collections.min(timeRanges));
    this.maxTime.set(timeRanges.isEmpty() ? EMPTY_TIME_RANGE : Collections.max(timeRanges));
    this.buffers.remove(timeRange);
  }

  @Override
  public void notifyCreate(TimeRange timeRange, ByteBuffer buffer)
  {
    this.buffers.putIfAbsent(timeRange, new CopyOnWriteArrayList<ByteBuffer>());
    this.buffers.get(timeRange).add(buffer);
    this.minTime.set(Collections.min(buffers.keySet()));
    this.maxTime.set(Collections.max(buffers.keySet()));
  }

  private Set<Long> getTimes(TimeRange timeRange)
  {
    if (timeRange != null)
    {
      Set<Long> times = new HashSet<Long>();
      for (long i = timeRange.getStart(); i <= timeRange.getEnd(); i++)
      {
        times.add(i);
      }
      return times;
    } else
    {
      Set<Long> times = new HashSet<Long>();
      for (long i = getMinTime(); i <= getMaxTime(); i++)
      {
        times.add(i);
      }
      return times;
    }
  }

}
