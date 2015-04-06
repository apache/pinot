package com.linkedin.thirdeye.views;

import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import io.dropwizard.views.View;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.ArrayList;
import java.util.List;

public class DefaultLandingView extends View
{
  private final StarTreeConfig config;
  private final String feedbackAddress;
  private final DateTime minTime;
  private final DateTime maxTime;

  public DefaultLandingView(StarTreeConfig config, String feedbackAddress, DateTime minTime, DateTime maxTime)
  {
    super("default-landing-view.ftl");
    this.config = config;
    this.feedbackAddress = feedbackAddress;
    this.minTime = minTime;
    this.maxTime = maxTime;
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

  public String getMaxTime()
  {
    return maxTime.toDateTime(DateTimeZone.UTC).toString();
  }

  public String getMinTime()
  {
    return minTime.toDateTime(DateTimeZone.UTC).toString();
  }
}
