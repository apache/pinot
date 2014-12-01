package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class StarTreeRecordStoreBlackHoleImpl implements StarTreeRecordStore
{
  private static final List<StarTreeRecord> EMPTY_RECORDS = new ArrayList<StarTreeRecord>(0);
  private static final byte[] EMPTY_ENCODED_STORE = new byte[0];
  private static final Set<String> EMPTY_DIMENSION_VALUES = new HashSet<String>();

  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final int[] emptyMetrics;

  public StarTreeRecordStoreBlackHoleImpl(List<String> dimensionNames, List<String> metricNames)
  {
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.emptyMetrics = new int[metricNames.size()];
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
  public long getByteCount()
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
  public int[] getMetricSums(StarTreeQuery query)
  {
    return emptyMetrics;
  }

  @Override
  public int getEntrySize()
  {
    return 0;
  }

  @Override
  public byte[] encode()
  {
    return EMPTY_ENCODED_STORE;
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
  public List<StarTreeRecord> getTimeSeries(StarTreeQuery query)
  {
    return EMPTY_RECORDS;
  }
}
