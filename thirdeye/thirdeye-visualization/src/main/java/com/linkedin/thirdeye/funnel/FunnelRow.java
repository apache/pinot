package com.linkedin.thirdeye.funnel;

import com.linkedin.thirdeye.api.MetricSpec;

public class FunnelRow
{
  private final MetricSpec metric;
  private final Double startRatio;
  private final Double endRatio;

  public FunnelRow(MetricSpec metric, Double startRatio, Double endRatio)
  {
    this.metric = metric;
    this.startRatio = startRatio;
    this.endRatio = endRatio;
  }

  public MetricSpec getMetric()
  {
    return metric;
  }

  public Double getStartRatio()
  {
    return startRatio;
  }

  public Double getEndRatio()
  {
    return endRatio;
  }

  public static FunnelRow createTopRow(MetricSpec metric)
  {
    return new FunnelRow(metric, 1.0, 1.0);
  }
}
