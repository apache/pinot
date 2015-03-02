package com.linkedin.thirdeye.views;

import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.heatmap.HeatMapCell;
import io.dropwizard.views.View;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultDashboardView extends View
{
  private final StarTreeConfig config;
  private final String metricName;
  private final Long dateTimeMillis;
  private final List<List<String>> disabledDimensions;
  private final List<String> activeDimension;
  private final TimeSeriesComponentView timeSeriesComponentView;
  private final HeatMapComponentView heatMapComponentView;
  private final String feedbackAddress;

  public DefaultDashboardView(StarTreeConfig config,
                              String metricName,
                              Long dateTimeMillis,
                              List<List<String>> disabledDimensions,
                              List<String> activeDimension,
                              TimeSeriesComponentView timeSeriesComponentView,
                              HeatMapComponentView heatMapComponentView,
                              String feedbackAddress)
  {
    super("default-dashboard-view.ftl");
    this.config = config;
    this.metricName = metricName;
    this.dateTimeMillis = dateTimeMillis;
    this.disabledDimensions = disabledDimensions;
    this.activeDimension = activeDimension;
    this.timeSeriesComponentView = timeSeriesComponentView;
    this.heatMapComponentView = heatMapComponentView;
    this.feedbackAddress = feedbackAddress;
  }

  public String getCollection()
  {
    return config.getCollection();
  }

  public String getPrimaryMetricName()
  {
    return metricName;
  }

  public Long getDateTimeMillis()
  {
    return dateTimeMillis;
  }

  public List<List<String>> getDisabledDimensions()
  {
    return disabledDimensions;
  }

  public List<String> getActiveDimension()
  {
    return activeDimension;
  }

  public List<String> getMetricNames()
  {
    List<String> metricNames = new ArrayList<String>();

    for (MetricSpec metricSpec : config.getMetrics())
    {
      metricNames.add(metricSpec.getName());
    }

    return metricNames;
  }

  public String getFeedbackAddress()
  {
    return feedbackAddress;
  }

  public String getFlotJsonData() throws IOException
  {
    return timeSeriesComponentView.getFlotJsonData();
  }

  public Map<String, List<List<HeatMapCell>>> getHeatMaps() throws Exception
  {
    return heatMapComponentView.getHeatMaps();
  }
}
