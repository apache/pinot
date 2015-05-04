package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.StarTreeRecordImpl;

import java.io.IOException;
import java.util.*;

public class StarTreeRecordStoreDefaultImpl implements StarTreeRecordStore
{
  private final StarTreeConfig config;
  private final DimensionStore dimensionStore;
  private final MetricStore metricStore;

  public StarTreeRecordStoreDefaultImpl(StarTreeConfig config,
                                        DimensionStore dimensionStore,
                                        MetricStore metricStore)
  {
    this.config = config;
    this.dimensionStore = dimensionStore;
    this.metricStore = metricStore;
  }

  @Override
  public void update(StarTreeRecord record)
  {
    Map<DimensionKey, Integer> logicalOffsets = dimensionStore.findMatchingKeys(record.getDimensionKey());
    for (Integer id : logicalOffsets.values())
    {
      metricStore.update(id, record.getMetricTimeSeries());
    }
  }

  // n.b. only combinations, no metrics
  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    List<DimensionKey> dimensionKeys = dimensionStore.getDimensionKeys();

    List<StarTreeRecord> records = new ArrayList<StarTreeRecord>(dimensionKeys.size());

    for (DimensionKey dimensionKey : dimensionKeys)
    {
      Map<DimensionKey, Integer> logicalOffsets = dimensionStore.findMatchingKeys(dimensionKey);
      MetricTimeSeries timeSeries = metricStore.getTimeSeries(logicalOffsets.values(), null);
      records.add(new StarTreeRecordImpl.Builder()
                          .setDimensionKey(dimensionKey)
                          .setMetricTimeSeries(timeSeries)
                          .build(config));
    }

    return records.iterator();
  }

  @Override
  public void clear()
  {
    metricStore.clear();
  }

  @Override
  public void open() throws IOException
  {
    // NOP (factory does this)
  }

  @Override
  public void close() throws IOException
  {
    // NOP (factory does this)
  }

  @Override
  public int getRecordCount()
  {
    return dimensionStore.getDimensionKeyCount();
  }

  @Override
  public int getRecordCountEstimate()
  {
    return dimensionStore.getDimensionKeyCount();
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    return dimensionStore.getDictionary().getCardinality(dimensionName);
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    return getMaxCardinalityDimension(null);
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
  {
    String maxDimensionName = null;
    Integer maxCardinality = null;

    for (DimensionSpec dimensionSpec : config.getDimensions())
    {
      int cardinality = getCardinality(dimensionSpec.getName());

      if ((blacklist == null || !blacklist.contains(dimensionSpec.getName()))
              && (maxCardinality == null || cardinality > maxCardinality))
      {
        maxCardinality = cardinality;
        maxDimensionName = dimensionSpec.getName();
      }
    }

    return maxDimensionName;
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    return dimensionStore.getDictionary().getDimensionValues(dimensionName);
  }

  @Override
  public Number[] getMetricSums(StarTreeQuery query)
  {
    return doQuery(query).getMetricSums();
  }

  @Override
  public Long getMinTime()
  {
    return metricStore.getMinTime();
  }

  @Override
  public Long getMaxTime()
  {
    return metricStore.getMaxTime();
  }

  @Override
  public Map<TimeRange, Integer> getTimeRangeCount()
  {
    return metricStore.getTimeRangeCount();
  }

  @Override
  public MetricTimeSeries getTimeSeries(StarTreeQuery query)
  {
    return doQuery(query);
  }

  @Override
  public Map<String, Map<String, Integer>> getForwardIndex()
  {
    return dimensionStore.getDictionary().asMap();
  }

  private MetricTimeSeries doQuery(StarTreeQuery query)
  {
    Map<DimensionKey, Integer> logicalOffsets = dimensionStore.findMatchingKeys(query.getDimensionKey());
    return metricStore.getTimeSeries(logicalOffsets.values(), query.getTimeRange());
  }
}
