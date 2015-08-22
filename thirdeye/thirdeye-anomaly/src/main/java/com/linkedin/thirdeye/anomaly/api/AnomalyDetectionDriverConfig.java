package com.linkedin.thirdeye.anomaly.api;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class represents configurations specific to a collection. It is mapped in from the configuration file.
 */
public class AnomalyDetectionDriverConfig {

  /** The name of the collection */
  private String collectionName;

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

  /** Prune exploration based on feedback to reduce computation costs and raising redundant anomalies */
  private boolean pruneExplortaionUsingFeedback = false;

  @JsonProperty
  public String getCollectionName() {
    return collectionName;
  }

  @JsonProperty
  public double getContributionMinProportion() {
    return contributionMinProportion;
  }

  @JsonProperty
  public String getContributionEstimateMetric() {
    return contributionEstimateMetric;
  }

  @JsonProperty
  public int getMaxExplorationDepth() {
    return maxExplorationDepth;
  }

  @JsonProperty
  public boolean isPruneExplortaionUsingFeedback() {
    return pruneExplortaionUsingFeedback;
  }

  @JsonProperty
  public List<String> getDimensionPrecedence() {
    return dimensionPrecedence;
  }

  public void setDimensionPrecedence(List<String> explorationPrecedence) {
    this.dimensionPrecedence = explorationPrecedence;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public void setContributionEstimateMetric(String contributionEstimateMetric) {
    this.contributionEstimateMetric = contributionEstimateMetric;
  }

  public void setContributionMinProportion(double contributionMinProportion) {
    this.contributionMinProportion = contributionMinProportion;
  }

  public void setMaxExplorationDepth(int maxExplorationDepth) {
    this.maxExplorationDepth = maxExplorationDepth;
  }

  public void setPruneExplortaionUsingFeedback(boolean pruneExplortaionUsingFeedback) {
    this.pruneExplortaionUsingFeedback = pruneExplortaionUsingFeedback;
  }
}
