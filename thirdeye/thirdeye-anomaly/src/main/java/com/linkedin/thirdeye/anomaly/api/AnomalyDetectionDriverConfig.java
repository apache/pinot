package com.linkedin.thirdeye.anomaly.api;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 *
 */
public class AnomalyDetectionDriverConfig {

  private String name;
  private String host;
  private short port;

  private double contributionMinProportion = 0.005;

  private boolean autoEstimateAnomalyVolume = true;

  private int maxExplorationDepth = 1;

  private boolean suppressSecondaryAnomalies = true;

  private List<String> neverExplore;

  @JsonProperty
  public String getName() {
    return name;
  }

  @JsonProperty
  public short getPort() {
    return port;
  }

  @JsonProperty
  public String getHost() {
    return host;
  }

  @JsonProperty
  public List<String> getNeverExplore() {
    return neverExplore;
  }

  @JsonProperty
  public double getContributionMinProportion() {
    return contributionMinProportion;
  }

  @JsonProperty
  public boolean isSuppressSecondaryAnomalies() {
    return suppressSecondaryAnomalies;
  }

  @JsonProperty
  public int getMaxExplorationDepth() {
    return maxExplorationDepth;
  }

  @JsonProperty
  public boolean autoEstimateAnomalyVolume() {
    return autoEstimateAnomalyVolume;
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

    for (String dimToNeverExplore : neverExplore) {
      if (dimToNeverExplore.equalsIgnoreCase(dimension)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param collections
   * @param name
   * @return
   *  Looks up the AnomalyDetectionDriverConfig in a list
   */
  public static AnomalyDetectionDriverConfig find(List<AnomalyDetectionDriverConfig> collections, String name) {
    for (AnomalyDetectionDriverConfig collection : collections) {
      if (name.equalsIgnoreCase(collection.getName())) {
        return collection;
      }
    }
    return null;
  }
}
