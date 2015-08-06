package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.nio.ByteBuffer;
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

public class MetricStoreImmutableImplOld implements MetricStore, MetricStoreListener
{
  private static final TimeRange EMPTY_TIME_RANGE = new TimeRange(-1L, -1L);

  private final StarTreeConfig config;
  private final MetricSchema metricSchema;

  private ConcurrentMap<TimeRange, List<ByteBuffer>> buffers;
  private final AtomicReference<TimeRange> minTime;
  private final AtomicReference<TimeRange> maxTime;
  private ConcurrentMap<TimeRange, Integer> timeRangeCount;

  public MetricStoreImmutableImplOld(StarTreeConfig config, ConcurrentMap<TimeRange, List<ByteBuffer>> buffers)
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

    for (Map.Entry<TimeRange, List<ByteBuffer>> entry : buffers.entrySet())
    {
      TimeRange bufferTimeRange = entry.getKey();

      int rowSize = bufferTimeRange.totalBuckets() * (Long.SIZE / 8 + metricSchema.getRowSizeInBytes());

      for (ByteBuffer originalBuffer : entry.getValue())
      {
        ByteBuffer buffer = originalBuffer.duplicate();

        if (bufferTimeRange.getStart() >= 0 && (timeRange == null || !bufferTimeRange.isDisjoint(timeRange)))
        {
          for (Integer logicalOffset : logicalOffsets)
          {
            buffer.mark();
            buffer.position(logicalOffset * rowSize);

            for (int i = 0; i < bufferTimeRange.totalBuckets(); i++)
            {
              Long time = buffer.getLong();

              for (MetricSpec metricSpec : config.getMetrics())
              {
                Number value = NumberUtils.readFromBuffer(buffer, metricSpec.getType());

                if (times.contains(time))
                {
                  timeSeries.increment(time, metricSpec.getName(), value);
                }
              }
            }
          }
        }
      }
    }

    return timeSeries;
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
  public Map<TimeRange, Integer> getTimeRangeCount() {

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
    }
    else
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
