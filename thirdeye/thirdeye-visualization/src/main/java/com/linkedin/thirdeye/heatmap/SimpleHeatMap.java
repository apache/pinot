package com.linkedin.thirdeye.heatmap;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class SimpleHeatMap implements HeatMap
{
  @Override
  public List<HeatMapCell> generateHeatMap(String metricName,
                                           Map<String, MetricTimeSeries> timeSeriesByDimensionValue,
                                           Long baselineTime,
                                           Long currentTime)
  {
    Map<String, Number> baseline = new HashMap<String, Number>();
    Map<String, Number> current = new HashMap<String, Number>();

    for (Map.Entry<String, MetricTimeSeries> entry : timeSeriesByDimensionValue.entrySet())
    {
      MetricTimeSeries timeSeries = entry.getValue();
      MetricType metricType = timeSeries.getSchema().getMetricType(metricName);

      sumByDimension(metricName, metricType, entry.getKey(), baseline, baselineTime, timeSeries);
      sumByDimension(metricName, metricType, entry.getKey(), current, currentTime, timeSeries);
    }

    return generateHeatMap(baseline, current);
  }

  private static void sumByDimension(String metricName,
                                     MetricType metricType,
                                     String dimensionValue,
                                     Map<String, Number> aggregates,
                                     Long time,
                                     MetricTimeSeries timeSeries)
  {


    Number increment = timeSeries.get(time, metricName);
    Number aggregate = aggregates.get(dimensionValue);
    if (aggregate == null)
    {
      aggregate = 0;
    }
    aggregates.put(dimensionValue, NumberUtils.sum(aggregate, increment, metricType));

  }

  protected abstract List<HeatMapCell> generateHeatMap(Map<String, Number> baseline, Map<String, Number> current);
}
