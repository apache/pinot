package com.linkedin.thirdeye.views;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.NumberUtils;
import com.linkedin.thirdeye.util.QueryUtils;
import io.dropwizard.views.View;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TimeSeriesComponentView extends View
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final StarTreeConfig config;
  private final List<String> metricNames;
  private final Map<DimensionKey, MetricTimeSeries> data;
  private final Long start;
  private final Long end;

  public TimeSeriesComponentView(StarTreeConfig config,
                                 List<String> metricNames,
                                 Map<DimensionKey, MetricTimeSeries> data,
                                 Long start,
                                 Long end)
  {
    super("time-series-component.ftl");
    this.config = config;
    this.data = data;
    this.start = start;
    this.end = end;

    if (metricNames == null)
    {
      this.metricNames = new ArrayList<String>();
      for (MetricSpec metricSpec : config.getMetrics())
      {
        this.metricNames.add(metricSpec.getName());
      }
    }
    else
    {
      this.metricNames = metricNames;
    }
  }

  public Long getStart()
  {
    return start;
  }

  public Long getEnd()
  {
    return end;
  }

  public String getFlotJsonData() throws IOException
  {
    List<Map<String, Object>> flotSeries = new ArrayList<Map<String, Object>>();

    for (Map.Entry<DimensionKey, MetricTimeSeries> entry : data.entrySet())
    {
      List<Long> times = new ArrayList<Long>(entry.getValue().getTimeWindowSet());
      Collections.sort(times);

      for (String metricName : metricNames)
      {
        Number[][] data = new Number[times.size()][];

        for (int i = 0; i < times.size(); i++)
        {
          long time = times.get(i);
          data[i] = new Number[] { time, entry.getValue().get(time, metricName) };
        }

        // Get ratio
        Number startValue = entry.getValue().get(start, metricName);
        Number endValue = entry.getValue().get(end, metricName);
        Double ratio = Double.POSITIVE_INFINITY;
        MetricType metricType = entry.getValue().getSchema().getMetricType(metricName);
        if (!NumberUtils.isZero(startValue, metricType))
        {
          ratio = 100 * (endValue.doubleValue() - startValue.doubleValue()) / startValue.doubleValue();
        }

        Map<String, Object> series = new HashMap<String, Object>();
        series.put("metricName", metricName);
        series.put("label", String.format("(%.2f)%% %s (%s)", ratio, metricName, entry.getKey()));
        series.put("dimensionValues", QueryUtils.convertDimensionKey(config.getDimensions(), entry.getKey()));
        series.put("data", data);

        flotSeries.add(series);
      }
    }

    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(flotSeries);
  }
}
