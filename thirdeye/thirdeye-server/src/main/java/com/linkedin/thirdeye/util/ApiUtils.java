package com.linkedin.thirdeye.util;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;
import com.linkedin.thirdeye.impl.MetricTimeSeriesUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ApiUtils
{
  public static List<ThirdEyeTimeSeries> convertTimeSeries(StarTreeConfig config,
                                                            List<String> metricNames,
                                                            Long timeWindow,
                                                            Map<DimensionKey, MetricTimeSeries> original)
  {
    List<ThirdEyeTimeSeries> result = new ArrayList<ThirdEyeTimeSeries>(original.size());

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : original.entrySet())
    {
      MetricTimeSeries timeSeries;
      if (timeWindow == null)
      {
        timeSeries = entry.getValue();
      }
      else
      {
        timeSeries = MetricTimeSeriesUtils.aggregate(entry.getValue(), timeWindow, Long.MAX_VALUE);
      }

      List<Long> times = new ArrayList<Long>(timeSeries.getTimeWindowSet());
      Collections.sort(times);

      for (String metricName : metricNames)
      {
        ThirdEyeTimeSeries resultPart = new ThirdEyeTimeSeries();
        resultPart.setDimensionValues(QueryUtils.convertDimensionKey(config.getDimensions(), entry.getKey()));
        resultPart.setLabel(metricName);

        Number[][] data = new Number[times.size()][];

        for (int i = 0; i < times.size(); i++)
        {
          long time = times.get(i);
          data[i] = new Number[] { time, entry.getValue().get(time, metricName) };
        }

        resultPart.setData(data);

        result.add(resultPart);
      }
    }

    return result;
  }
}
