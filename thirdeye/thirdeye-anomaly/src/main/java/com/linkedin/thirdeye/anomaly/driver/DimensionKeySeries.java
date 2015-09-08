package com.linkedin.thirdeye.anomaly.driver;

import com.linkedin.thirdeye.api.DimensionKey;

/**
 * Class representing a series to run.
 */
public class DimensionKeySeries {

  private final DimensionKey dimensionKey;
  private final double contributionEstimate;

  public DimensionKeySeries(DimensionKey dimensionKey, double contributionEstimate) {
    super();
    this.dimensionKey = dimensionKey;
    this.contributionEstimate = contributionEstimate;
  }

  public double getContributionEstimate() {
    return contributionEstimate;
  }

  public DimensionKey getDimensionKey() {
    return dimensionKey;
  }

  @Override
  public String toString() {
    return "DimensionKeySeries [dimensionKey=" + dimensionKey + ", contributionEstimate=" + contributionEstimate + "]";
  }

}