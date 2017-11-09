package com.linkedin.thirdeye.common;

public enum FactorySource {
  ALERT_FETCHER("com.linkedin.thirdeye.alert.fetcher"),
  ANOMALY_FEED("com.linkedin.thirdeye.alert.feed"),
  EMAIL_CONTENT_FORMATTER("com.linkedin.thirdeye.alert.content");

  private String packagePath;
  FactorySource(String packagePath) {
    this.packagePath = packagePath;
  }
  public String getPackagePath(){
    return packagePath;
  }
}
