package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.TimeRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarTreeRecordStoreBlackHoleImpl implements StarTreeRecordStore
{
  private static final List<StarTreeRecord> EMPTY_RECORDS = new ArrayList<StarTreeRecord>(0);
  private static final byte[] EMPTY_ENCODED_STORE = new byte[0];
  private static final Set<String> EMPTY_DIMENSION_VALUES = new HashSet<String>();

  private final MetricSchema metricSchema;
  private final Number[] emptyMetrics;

  public StarTreeRecordStoreBlackHoleImpl(List<MetricSpec> metricSpecs)
  {
    this.metricSchema = MetricSchema.fromMetricSpecs(metricSpecs);
    this.emptyMetrics = new Number[metricSpecs.size()];
    Arrays.fill(emptyMetrics, 0);
  }

  @Override
  public void update(StarTreeRecord record)
  {
    // NOP
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    return EMPTY_RECORDS.iterator();
  }

  @Override
  public void clear()
  {
    // NOP
  }

  @Override
  public void open() throws IOException
  {
    // NOP
  }

  @Override
  public void close() throws IOException
  {
    // NOP
  }

  @Override
  public int getRecordCount()
  {
    return 0;
  }

  @Override
  public int getRecordCountEstimate()
  {
    return 0;
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    return 0;
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    return null;
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
  {
    return null;
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    return EMPTY_DIMENSION_VALUES;
  }

  @Override
  public Number[] getMetricSums(StarTreeQuery query)
  {
    return emptyMetrics;
  }

  @Override
  public Long getMinTime()
  {
    return null;
  }

  @Override
  public Long getMaxTime()
  {
    return null;
  }


  @Override
  public Map<TimeRange, Integer> getTimeRangeCount()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public MetricTimeSeries getTimeSeries(StarTreeQuery query)
  {
    return new MetricTimeSeries(metricSchema);
  }

  @Override
  public Map<String, Map<String, Integer>> getForwardIndex() {
    throw new UnsupportedOperationException("No forward index available in this implementation");
  }

}
