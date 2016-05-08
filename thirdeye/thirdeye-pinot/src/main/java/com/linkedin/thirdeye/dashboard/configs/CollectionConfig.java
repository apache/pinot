package com.linkedin.thirdeye.dashboard.configs;

public class CollectionConfig extends AbstractConfig {
  String collectionName;

  boolean isActive = true;

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

  @Override
  public String toJSON() throws Exception {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

}
