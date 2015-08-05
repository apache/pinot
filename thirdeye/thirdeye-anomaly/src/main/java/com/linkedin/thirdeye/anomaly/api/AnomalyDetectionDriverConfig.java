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

  /** The server hosting data */
  private String thirdEyeServerHost;

  /** The port to request data from the server */
  private short thirdEyeServerPort;

  /**
   * Metric with which to estimate the contribution of a dimension key, and also to apply the threshold for
   * exploration
   */
  private String contributionEstimateMetric;

  /** Minimum proportion that a series from a dimension key must contribute to be evaluated. */
  private double contributionMinProportion = 0.005;

  /** The maximum number of dimensions that the driver will attempt to group by during exploration. */
  private int maxExplorationDepth = 1;

  /** Suppress anomalies in series if a series that includes it is anomalous. */
  private boolean suppressSecondaryAnomalies = true;

  /** Set of dimensions that the driver should not attempt to group by. */
  private Set<String> neverExplore;

  @JsonProperty
  public String getCollectionName() {
    return collectionName;
  }

  @JsonProperty
  public short getThirdEyeServerPort() {
    return thirdEyeServerPort;
  }

  @JsonProperty
  public String getThirdEyeServerHost() {
    return thirdEyeServerHost;
  }

  @JsonProperty
  public Set<String> getNeverExplore() {
    return neverExplore;
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
  public boolean isSuppressSecondaryAnomalies() {
    return suppressSecondaryAnomalies;
  }

  @JsonProperty
  public int getMaxExplorationDepth() {
    return maxExplorationDepth;
  }

  /**
   * @param dimension
   * @return
   *  Whether a dimension should never be used in a group by
   */
  public boolean isNeverExploreDimension(String dimension) {
    // not using never explore
    if (neverExplore == null) {
      return false;
    }

    return neverExplore.contains(dimension);
  }

  /**
   * @param collections
   * @param name
   * @return
   *  Looks up the AnomalyDetectionDriverConfig in a list
   */
  public static AnomalyDetectionDriverConfig find(List<AnomalyDetectionDriverConfig> collections, String name) {
    for (AnomalyDetectionDriverConfig collection : collections) {
      if (name.equalsIgnoreCase(collection.getCollectionName())) {
        return collection;
      }
    }
    return null;
  }
}
