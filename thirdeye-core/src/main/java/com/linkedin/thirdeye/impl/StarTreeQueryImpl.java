package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class StarTreeQueryImpl implements StarTreeQuery
{
  private final Map<String, String> dimensionValues;
  private final Set<Long> timeBuckets;
  private final Map.Entry<Long, Long> timeRange;

  public StarTreeQueryImpl(Map<String, String> dimensionValues,
                           Set<Long> timeBuckets,
                           Map.Entry<Long, Long> timeRange)
  {
    this.dimensionValues = dimensionValues;
    this.timeBuckets = timeBuckets;
    this.timeRange = timeRange;
  }

  @Override
  public Set<String> getStarDimensionNames()
  {
    Set<String> starDimensions = new HashSet<String>();

    for (Map.Entry<String, String> entry : dimensionValues.entrySet())
    {
      if (StarTreeConstants.STAR.equals(entry.getValue()))
      {
        starDimensions.add(entry.getKey());
      }
    }

    return starDimensions;
  }

  @Override
  public Map<String, String> getDimensionValues()
  {
    return dimensionValues;
  }

  @Override
  public Set<Long> getTimeBuckets()
  {
    return timeBuckets;
  }

  @Override
  public Map.Entry<Long, Long> getTimeRange()
  {
    return timeRange;
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("dimensionValues=").append(dimensionValues);

    if (timeBuckets != null)
    {
      sb.append("timeBuckets=").append(timeBuckets);
    }

    if (timeRange != null)
    {
      sb.append("timeRange=").append(timeRange);
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof StarTreeQuery))
    {
      return false;
    }
    StarTreeQuery q = (StarTreeQuery) o;

    if (timeBuckets != null)
    {
      return dimensionValues.equals(q.getDimensionValues()) && timeBuckets.equals(q.getTimeBuckets());
    }
    else if (timeRange != null)
    {
      return dimensionValues.equals(q.getDimensionValues()) && timeRange.equals(q.getTimeRange());
    }
    else
    {
      return q.getDimensionValues().equals(dimensionValues);
    }
  }

  @Override
  public int hashCode()
  {
    return dimensionValues.hashCode();
  }

  public static class Builder
  {
    private final Map<String, String> dimensionValues = new HashMap<String, String>();
    private Set<Long> timeBuckets;
    private Map.Entry<Long, Long> timeRange;

    public Builder setDimensionValue(String dimensionName, String dimensionValue)
    {
      dimensionValues.put(dimensionName, dimensionValue);
      return this;
    }

    public Builder setDimensionValues(Map<String, String> dimensionValues)
    {
      this.dimensionValues.putAll(dimensionValues);
      return this;
    }

    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    public Set<Long> getTimeBuckets()
    {
      return timeBuckets;
    }

    public Builder setTimeBuckets(Set<Long> timeBuckets)
    {
      this.timeBuckets = timeBuckets;
      return this;
    }

    public Map.Entry<Long, Long> getTimeRange()
    {
      return timeRange;
    }

    public Builder setTimeRange(Long startTime, Long endTime)
    {
      Iterator<Map.Entry<Long, Long>> itr = Collections.singletonMap(startTime, endTime).entrySet().iterator();
      this.timeRange = itr.next();
      return this;
    }

    public Builder setTimeRange(Map.Entry<Long, Long> timeRange)
    {
      this.timeRange = timeRange;
      return this;
    }

    public StarTreeQuery build()
    {
      if (timeBuckets != null && timeRange != null)
      {
        throw new IllegalArgumentException("Can only specify one of timeBuckets or timeRange");
      }

      return new StarTreeQueryImpl(dimensionValues, timeBuckets, timeRange);
    }
  }
}
