package com.linkedin.thirdeye.views;

import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import io.dropwizard.views.View;

import java.util.ArrayList;
import java.util.List;

public class DefaultLandingView extends View
{
  private final StarTreeConfig config;
  private final String feedbackAddress;

  public DefaultLandingView(StarTreeConfig config, String feedbackAddress)
  {
    super("default-landing-view.ftl");
    this.config = config;
    this.feedbackAddress = feedbackAddress;
  }

  public String getCollection()
  {
    return config.getCollection();
  }

  public String getFeedbackAddress()
  {
    return feedbackAddress;
  }

  public String getPrimaryMetricName()
  {
    return config.getMetrics().get(0).getName();
  }

  public Long getDateTimeMillis()
  {
    return System.currentTimeMillis();
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
}
