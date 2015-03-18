package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarTreeRecordStoreCompositeImpl implements StarTreeRecordStore
{
  private final StarTreeConfig config;
  private final StarTreeRecordStore immutableStore;
  private final StarTreeRecordStore mutableStore;

  public StarTreeRecordStoreCompositeImpl(StarTreeConfig config,
                                          StarTreeRecordStore immutableStore,
                                          StarTreeRecordStore mutableStore)
  {
    this.config = config;
    this.immutableStore = immutableStore;
    this.mutableStore = mutableStore;
  }

  @Override
  public void update(StarTreeRecord record)
  {
    mutableStore.update(record);
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    List<StarTreeRecord> records = new LinkedList<StarTreeRecord>();

    Iterator<StarTreeRecord> itr = immutableStore.iterator();
    while (itr.hasNext())
    {
      records.add(itr.next());
    }

    itr = mutableStore.iterator();
    while (itr.hasNext())
    {
      records.add(itr.next());
    }

    return records.iterator();
  }

  @Override
  public void clear()
  {
    // n.b. clear is meaningless for immutable store
    mutableStore.clear();
  }

  @Override
  public void open() throws IOException
  {
    immutableStore.open();
    mutableStore.open();
  }

  @Override
  public void close() throws IOException
  {
    immutableStore.close();
    mutableStore.close();
  }

  @Override
  public int getRecordCount()
  {
    return getDimensionKeys().size();
  }

  @Override
  public int getRecordCountEstimate()
  {
    // This may be a little off because we may be double counting dimension combinations
    // across stores, and within the mutable record store (but that's why it's an estimate)
    return immutableStore.getRecordCountEstimate() + mutableStore.getRecordCountEstimate();
  }

  @Override
  public int getCardinality(String dimensionName)
  {
    return getDimensionValues(dimensionName).size();
  }

  @Override
  public String getMaxCardinalityDimension()
  {
    return getMaxCardinalityDimension(null);
  }

  @Override
  public String getMaxCardinalityDimension(Collection<String> blacklist)
  {
    String maxCardinalityDimension = null;
    int maxCardinality = -1;

    for (DimensionSpec dimensionSpec : config.getDimensions())
    {
      if (blacklist == null || !blacklist.contains(dimensionSpec.getName()))
      {
        int cardinality = getDimensionValues(dimensionSpec.getName()).size();
        if (cardinality > maxCardinality)
        {
          maxCardinality = cardinality;
          maxCardinalityDimension = dimensionSpec.getName();
        }
      }
    }

    return maxCardinalityDimension;
  }

  @Override
  public Set<String> getDimensionValues(String dimensionName)
  {
    Set<String> dimensionValues = new HashSet<String>();
    dimensionValues.addAll(immutableStore.getDimensionValues(dimensionName));
    dimensionValues.addAll(mutableStore.getDimensionValues(dimensionName));
    return dimensionValues;
  }

  @Override
  public Number[] getMetricSums(StarTreeQuery query)
  {
    Number[] fromImmutable = immutableStore.getMetricSums(query);
    Number[] fromMutable = mutableStore.getMetricSums(query);

    if (fromImmutable.length != fromMutable.length)
    {
      throw new IllegalStateException("Record stores have differing number of metrics");
    }

    if (fromImmutable.length != config.getMetrics().size())
    {
      throw new IllegalStateException("Record store did not return "
                                              + config.getMetrics().size() + " metrics for " + query);
    }

    for (int i = 0; i < config.getMetrics().size(); i++)
    {
      fromImmutable[i] = NumberUtils.sum(fromImmutable[i], fromMutable[i], config.getMetrics().get(i).getType());
    }

    return fromImmutable;
  }

  @Override
  public Long getMinTime()
  {
    long fromImmutable = immutableStore.getMinTime();
    long fromMutable = mutableStore.getMinTime();

    if (fromImmutable == -1 && fromMutable == -1)
    {
      return -1L;
    }
    else if (fromImmutable != -1 && fromMutable == -1)
    {
      return fromImmutable;
    }
    else if (fromImmutable == -1 && fromMutable != -1)
    {
      return fromMutable;
    }
    else
    {
      return fromImmutable < fromMutable ? fromImmutable : fromMutable;
    }
  }

  @Override
  public Long getMaxTime()
  {
    long fromImmutable = immutableStore.getMaxTime();
    long fromMutable = mutableStore.getMaxTime();

    if (fromImmutable == -1 && fromMutable == -1)
    {
      return -1L;
    }
    else if (fromImmutable != -1 && fromMutable == -1)
    {
      return fromImmutable;
    }
    else if (fromImmutable == -1 && fromMutable != -1)
    {
      return fromMutable;
    }
    else
    {
      return fromImmutable > fromMutable ? fromImmutable : fromMutable;
    }
  }

  @Override
  public MetricTimeSeries getTimeSeries(StarTreeQuery query)
  {
    MetricTimeSeries fromImmutable = immutableStore.getTimeSeries(query);
    MetricTimeSeries fromMutable = mutableStore.getTimeSeries(query);
    fromImmutable.aggregate(fromMutable);
    return fromImmutable;
  }

  @Override
  public Map<String, Map<String, Integer>> getForwardIndex()
  {
    throw new UnsupportedOperationException("Cannot get forward index " +
                                                    "because composite record stores may use different encodings");
  }

  private Set<DimensionKey> getDimensionKeys()
  {
    Set<DimensionKey> dimensionKeys = new HashSet<DimensionKey>();

    for (StarTreeRecord record : this)
    {
      dimensionKeys.add(record.getDimensionKey());
    }

    return dimensionKeys;
  }
}
