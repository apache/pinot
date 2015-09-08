package com.linkedin.thirdeye.anomaly.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 * This class represents configurations specific to a collection. It is mapped in from the configuration file.
 */
public class AnomalyDetectionDriverConfig {

  /**
   * Metric with which to estimate the contribution of a dimension key, and also to apply the threshold for
   * exploration
   */
  private String contributionEstimateMetric;

  /** Minimum proportion that a series from a dimension key must contribute to be evaluated. */
  private double contributionMinProportion = 0.005;

  /** The maximum number of dimensions that the driver will attempt to group by during exploration. */
  private int maxExplorationDepth = 1;

  /**
   * Precedence in exploring the dimensions
   * It is recommended that high cardinality dimensions have high precedence.
   */
  private List<String> dimensionPrecedence;

  /**
   * Prune exploration based on feedback to reduce computation costs and raising redundant anomalies.
   * TODO: This field is ignored.
   */
  private boolean pruneExplortaionUsingFeedback = false;

  /** The amount of time the driver should use to make decisions regarding exploration. */
  private TimeGranularity driverTimeWindow = new TimeGranularity(7, TimeUnit.DAYS);

  @JsonProperty
  public String getContributionEstimateMetric() {
    return contributionEstimateMetric;
  }

  public void setContributionEstimateMetric(String contributionEstimateMetric) {
    this.contributionEstimateMetric = contributionEstimateMetric;
  }

  @JsonProperty
  public double getContributionMinProportion() {
    return contributionMinProportion;
  }

  public void setContributionMinProportion(double contributionMinProportion) {
    this.contributionMinProportion = contributionMinProportion;
  }

  @JsonProperty
  public int getMaxExplorationDepth() {
    return maxExplorationDepth;
  }

  public void setMaxExplorationDepth(int maxExplorationDepth) {
    this.maxExplorationDepth = maxExplorationDepth;
  }

  @JsonProperty
  public List<String> getDimensionPrecedence() {
    return dimensionPrecedence;
  }

  public void setDimensionPrecedence(List<String> dimensionPrecedence) {
    this.dimensionPrecedence = dimensionPrecedence;
  }

  @JsonProperty
  public boolean isPruneExplortaionUsingFeedback() {
    return pruneExplortaionUsingFeedback;
  }

  public void setPruneExplortaionUsingFeedback(boolean pruneExplortaionUsingFeedback) {
    this.pruneExplortaionUsingFeedback = pruneExplortaionUsingFeedback;
  }

  @JsonProperty
  public TimeGranularity getDriverTimeWindow() {
    return driverTimeWindow;
  }

  public void setDriverTimeWindow(TimeGranularity driverTimeWindow) {
    this.driverTimeWindow = driverTimeWindow;
  }


}
