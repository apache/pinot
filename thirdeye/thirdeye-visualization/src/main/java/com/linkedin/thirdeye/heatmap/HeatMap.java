package com.linkedin.thirdeye.heatmap;

import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.TimeRange;

import java.util.List;
import java.util.Map;

public interface HeatMap
{
  List<HeatMapCell> generateHeatMap(String metricName,
                                    Map<String, MetricTimeSeries> timeSeriesByDimensionValue,
                                    Long baselineTime,
                                    Long currentTime);
}
