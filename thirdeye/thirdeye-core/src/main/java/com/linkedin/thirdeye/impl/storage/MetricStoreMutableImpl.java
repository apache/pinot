package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeRange;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class MetricStoreMutableImpl implements MetricStore
{
  private final MetricSchema metricSchema;
  private final ConcurrentMap<Integer, MetricTimeSeries> store;
  private final AtomicLong minTime;
  private final AtomicLong maxTime;

  public MetricStoreMutableImpl(StarTreeConfig starTreeConfig)
  {
    this.metricSchema = MetricSchema.fromMetricSpecs(starTreeConfig.getMetrics());
    this.store = new ConcurrentHashMap<Integer, MetricTimeSeries>();
    this.minTime = new AtomicLong(-1);
    this.maxTime = new AtomicLong(-1);
  }

  @Override
  public void update(int id, MetricTimeSeries timeSeries)
  {
    MetricTimeSeries aggregateSeries = store.get(id);

    if (aggregateSeries == null)
    {
      store.putIfAbsent(id, new MetricTimeSeries(metricSchema));
      aggregateSeries = store.get(id);
    }

    aggregateSeries.aggregate(timeSeries);

    for (Long time : timeSeries.getTimeWindowSet())
    {
      setIfLess(minTime, time);
      setIfGreater(maxTime, time);
    }
  }

  @Override
  public MetricTimeSeries getTimeSeries(List<Integer> logicalOffsets, TimeRange timeRange)
  {
    MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);

    for (Integer id : logicalOffsets)
    {
      MetricTimeSeries incrementalSeries = store.get(id);
      if (incrementalSeries != null)
      {
        if (timeRange != null)
        {
          timeSeries.aggregate(incrementalSeries, timeRange);
        }
        else
        {
          timeSeries.aggregate(incrementalSeries);
        }
      }
    }

    return timeSeries;
  }

  @Override
  public Long getMinTime()
  {
    return minTime.get();
  }

  @Override
  public Long getMaxTime()
  {
    return maxTime.get();
  }

  @Override
  public Map<TimeRange, Integer> getTimeRangeCount() {

    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    store.clear();
  }

  private static void setIfLess(AtomicLong target, Long value)
  {
    while (true)
    {
      Long current = target.get();

      if (current == -1 || value < current)
      {
        if (target.compareAndSet(current, value))
        {
          return;
        }
        // else, retry
      }
      else
      {
        return;
      }
    }
  }

  private static void setIfGreater(AtomicLong target, Long value)
  {
    while (true)
    {
      Long current = target.get();

      if (current == -1 || value > current)
      {
        if (target.compareAndSet(current, value))
        {
          return;
        }
        // else, retry
      }
      else
      {
        return;
      }
    }
  }

}
