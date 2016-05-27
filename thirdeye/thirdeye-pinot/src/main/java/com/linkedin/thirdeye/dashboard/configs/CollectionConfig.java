package com.linkedin.thirdeye.dashboard.configs;

import java.util.Map;

public class CollectionConfig extends AbstractConfig {
  String collectionName;

  boolean isActive = true;
  boolean enableCount = false; // Default __COUNT metric

  Map<String, String> derivedMetrics;

  public CollectionConfig() {

  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public boolean isActive() {
    return isActive;
  }

  public void setActive(boolean isActive) {
    this.isActive = isActive;
  }

  public boolean isEnableCount() {
    return enableCount;
  }

  public void setEnableCount(boolean enableCount) {
    this.enableCount = enableCount;
  }

  public Map<String, String> getDerivedMetrics() {
    return derivedMetrics;
  }

  public void setDerivedMetrics(Map<String, String> derivedMetrics) {
    this.derivedMetrics = derivedMetrics;
  }

  @Override
  public String toJSON() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

}
