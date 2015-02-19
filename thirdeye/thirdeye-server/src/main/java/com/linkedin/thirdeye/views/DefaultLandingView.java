package com.linkedin.thirdeye.views;

import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import io.dropwizard.views.View;

import java.util.ArrayList;
import java.util.List;

public class DefaultLandingView extends View
{
  private final StarTreeConfig config;

  public DefaultLandingView(StarTreeConfig config)
  {
    super("default-landing-view.ftl");
    this.config = config;
  }

  public String getCollection()
  {
    return config.getCollection();
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
