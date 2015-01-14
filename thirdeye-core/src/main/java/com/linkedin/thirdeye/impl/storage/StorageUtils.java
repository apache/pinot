package com.linkedin.thirdeye.impl.storage;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class StorageUtils
{
  /** Adds a dimension combination to the end of a dimension store */
  public static void addToDimensionStore(StarTreeConfig config,
                                         ByteBuffer buffer,
                                         DimensionKey dimensionKey,
                                         DimensionDictionary dictionary)
  {
    for (int i = 0; i < config.getDimensions().size(); i++)
    {
      String dimensionName = config.getDimensions().get(i).getName();
      String dimensionValue = dimensionKey.getDimensionValues()[i];
      Integer valueId = dictionary.getValueId(dimensionName, dimensionValue);
      buffer.putInt(valueId);
    }
  }

  /** Adds a metric time series to the end of a metric store */
  public static void addToMetricStore(StarTreeConfig config,
                                      ByteBuffer buffer,
                                      MetricTimeSeries timeSeries)
  {
    List<Long> times = new ArrayList<Long>(timeSeries.getTimeWindowSet());
    Collections.sort(times);
    for (Long time : times)
    {
      buffer.putLong(time);
      for (MetricSpec metricSpec : config.getMetrics())
      {
        Number value = timeSeries.get(time, metricSpec.getName());
        NumberUtils.addToBuffer(buffer, value, metricSpec.getType());
      }
    }
  }
}
