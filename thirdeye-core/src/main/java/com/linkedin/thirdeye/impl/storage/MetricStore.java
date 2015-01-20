package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MetricStore
{
  private final StarTreeConfig config;
  private final MetricSchema metricSchema;
  private final Object sync;

  private Map<TimeRange, ByteBuffer> buffers;
  private List<TimeRange> timeRanges;

  public MetricStore(StarTreeConfig config, Map<TimeRange, ByteBuffer> buffers)
  {
    this.config = config;
    this.buffers = buffers;
    this.metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    this.sync = new Object();
    this.timeRanges = new ArrayList<TimeRange>(buffers.keySet());
    Collections.sort(timeRanges);
  }

  public MetricTimeSeries getTimeSeries(Collection<Integer> logicalOffsets, TimeRange timeRange)
  {
    synchronized (sync)
    {
      MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

      Set<Long> times = getTimes(timeRange);

      for (Long time : times)
      {
        for (MetricSpec metricSpec : config.getMetrics())
        {
          timeSeries.set(time, metricSpec.getName(), 0);
        }
      }

      for (Map.Entry<TimeRange, ByteBuffer> entry : buffers.entrySet())
      {
        TimeRange bufferTimeRange = entry.getKey();

        int rowSize = bufferTimeRange.totalBuckets() * (Long.SIZE / 8 + metricSchema.getRowSizeInBytes());

        ByteBuffer buffer = entry.getValue();

        buffer.rewind();

        if (timeRange == null || !bufferTimeRange.isDisjoint(timeRange))
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

            buffer.reset();
          }
        }
      }

      return timeSeries;
    }
  }

  public Long getMinTime()
  {
    synchronized (sync)
    {
      if (timeRanges.isEmpty())
      {
        return -1L;
      }
      return timeRanges.get(0).getStart();
    }
  }

  public Long getMaxTime()
  {
    synchronized (sync)
    {
      if (timeRanges.isEmpty())
      {
        return -1L;
      }
      return timeRanges.get(timeRanges.size() - 1).getEnd();
    }
  }

  public void notifyDelete(TimeRange timeRange)
  {
    synchronized (sync)
    {
      this.buffers.remove(timeRange);
      this.timeRanges = new ArrayList<TimeRange>(buffers.keySet());
      Collections.sort(this.timeRanges);
    }
  }

  public void notifyCreate(TimeRange timeRange, ByteBuffer buffer)
  {
    synchronized (sync)
    {
      this.buffers.put(timeRange, buffer);
      this.timeRanges = new ArrayList<TimeRange>(buffers.keySet());
      Collections.sort(this.timeRanges);
    }
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
