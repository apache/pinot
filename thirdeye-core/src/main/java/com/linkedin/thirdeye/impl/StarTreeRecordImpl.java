package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class StarTreeRecordImpl implements StarTreeRecord
{
  private final Map<String, String> dimensionValues;
  private final Map<String, Number> metricValues;
  private final Map<String, MetricType> metricTypes;

  private final Long time;

  private AtomicReference<String> key;
  private AtomicReference<String> timeKey;

  public StarTreeRecordImpl(Map<String, String> dimensionValues,
                            Map<String, Number> metricValues,
                            Map<String, MetricType> metricTypes, Long time)
  {
    this.dimensionValues = dimensionValues;
    this.metricValues = metricValues;
    this.metricTypes = metricTypes;
    this.time = time;
    this.key = new AtomicReference<String>();
    this.timeKey = new AtomicReference<String>();
    if(metricValues !=null && metricValues.size() != metricTypes.size()){
      throw new IllegalArgumentException("metric values  and metric types size must match");
    }
  }

  @Override
  public Map<String, String> getDimensionValues()
  {
    return dimensionValues;
  }

  @Override
  public Map<String, Number> getMetricValues()
  {
    return metricValues;
  }

  @Override
  public Map<String, MetricType> getMetricTypes()
  {
    return metricTypes;
  }
  @Override
  public Long getTime()
  {
    return time;
  }

  @Override
  public String getKey(boolean includeTime)
  {
    if (includeTime)
    {
      if (timeKey.get() == null)
      {
        timeKey.compareAndSet(null, dimensionValues + "@" + time);
      }
      return timeKey.get();
    }
    else
    {
      if (key.get() == null)
      {
        key.compareAndSet(null, dimensionValues.toString());
      }
      return key.get();
    }
  }

  @Override
  public StarTreeRecord relax(String dimensionName)
  {
    return relax(Arrays.asList(dimensionName));
  }

  @Override
  public StarTreeRecord relax(Collection<String> dimensionNames)
  {
    Builder builder = new Builder();

    for (Map.Entry<String, String> entry : dimensionValues.entrySet())
    {
      if (dimensionNames.contains(entry.getKey()))
      {
        builder.setDimensionValue(entry.getKey(), StarTreeConstants.STAR);
      }
      else
      {
        builder.setDimensionValue(entry.getKey(), entry.getValue());
      }
    }

    for (Map.Entry<String, Number> entry : metricValues.entrySet())
    {
      builder.setMetricValue(entry.getKey(), entry.getValue());
    }
    builder.setMetricType(metricTypes);
    builder.setTime(time);

    return builder.build();
  }

  @Override
  public StarTreeRecord copy(boolean keepMetrics)
  {
    Builder builder = new Builder();

    for (Map.Entry<String, String> entry : dimensionValues.entrySet())
    {
      builder.setDimensionValue(entry.getKey(), entry.getValue());
    }

    if (keepMetrics)
    {
      for (Map.Entry<String, Number> entry : metricValues.entrySet())
      {
        builder.setMetricValue(entry.getKey(), entry.getValue());
      }
    }
    builder.setMetricType(metricTypes);

    builder.setTime(time);

    return builder.build();
  }

  @Override
  public StarTreeRecord aliasOther(String dimensionName)
  {
    return aliasOther(Arrays.asList(dimensionName));
  }

  @Override
  public StarTreeRecord aliasOther(Collection<String> otherDimensionNames)
  {
    Builder builder = new Builder();

    for (Map.Entry<String, String> entry : dimensionValues.entrySet())
    {
      if (otherDimensionNames.contains(entry.getKey()))
      {
        builder.setDimensionValue(entry.getKey(), StarTreeConstants.OTHER);
      }
      else
      {
        builder.setDimensionValue(entry.getKey(), entry.getValue());
      }
    }

    for (Map.Entry<String, Number> entry : metricValues.entrySet())
    {
      builder.setMetricValue(entry.getKey(), entry.getValue());
    }
    builder.setMetricType(metricTypes);

    builder.setTime(time);

    return builder.build();
  }

  @Override
  public int hashCode()
  {
    return dimensionValues.hashCode() + 13 * (time == null ? 1 : time.hashCode());
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof StarTreeRecord))
    {
      return false;
    }
    StarTreeRecord r = (StarTreeRecord) o;

    boolean dimensionEquals = dimensionValues == null ? r.getDimensionValues() == null : dimensionValues.equals(r.getDimensionValues());
    boolean timeEquals = time == null ? r.getTime() == null : time.equals(r.getTime());

    return dimensionEquals && timeEquals;
  }

  @Override
  public String toString()
  {
    return new StringBuilder()
            .append("dimensions=")
            .append(dimensionValues)
            .append(";metrics=")
            .append(metricValues)
            .append(";time=")
            .append(time)
            .toString();
  }

  public static class Builder
  {
    private final Map<String, String> dimensionValues = new HashMap<String, String>();
    private final Map<String, Number> metricValues = new HashMap<String, Number>();
    private final Map<String, MetricType> metricTypes = new HashMap<String, MetricType>();

    private Long time;

    public Map<String, String> getDimensionValues()
    {
      return dimensionValues;
    }

    public Map<String, Number> getMetricValues()
    {
      return metricValues;
    }
    
    public Map<String, MetricType> getMetricTypes()
    {
      return metricTypes;
    }

    public Long getTime()
    {
      return time;
    }

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

    public Builder updateDimensionValues(Map<String, String> dimensionValues)
    {
      for (Map.Entry<String, String> entry : dimensionValues.entrySet())
      {
        String current = this.dimensionValues.get(entry.getKey());
        if (current == null)
        {
          this.dimensionValues.put(entry.getKey(), entry.getValue());
        }
        else if (!current.equals(entry.getValue()))
        {
          this.dimensionValues.put(entry.getKey(), StarTreeConstants.STAR);
        }
      }
      return this;
    }

    public Builder setMetricValue(String metricName, Number metricValue)
    {
      metricValues.put(metricName, metricValue);
      return this;
    }

    public Builder setMetricValues(Map<String, Number> metricValues)
    {
      this.metricValues.putAll(metricValues);
      return this;
    }

    public Builder updateMetricValues(Map<String, Number> metricValues)
    {
      for (Map.Entry<String, Number> entry : metricValues.entrySet())
      {
        String metricName = entry.getKey();
        Number current = this.metricValues.get(metricName);
        Number sum = NumberUtils.sum(current,entry.getValue(), metricTypes.get(metricName));
        this.metricValues.put(metricName, sum);
      }
      return this;
    }

    public Builder setMetricType(String metricName, MetricType metricType)
    {
      metricTypes.put(metricName, metricType);
      return this;
    }

    public Builder setMetricType(Map<String, MetricType> metricTypes)
    {
      this.metricTypes.putAll(metricTypes);
      return this;
    }
    
    public Builder setTime(Long time)
    {
      this.time = time;
      return this;
    }

    public StarTreeRecord build()
    {
      return new StarTreeRecordImpl(dimensionValues, metricValues, metricTypes, time);
    }

    public void clear()
    {
      this.time = null;
      this.dimensionValues.clear();
      this.metricValues.clear();
    }
  }

  @Override
  public int compareTo(StarTreeRecord starTreeRecord)
  {
    if (time == null)
    {
      return starTreeRecord.getTime() == null ? 0 : -1;
    }
    else
    {
      return starTreeRecord.getTime() == null ? 1 : (int) (time - starTreeRecord.getTime());
    }
  }
}
