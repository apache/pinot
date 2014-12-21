package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase2;

import java.util.List;

public class StarTreeBootstrapPhaseTwoConfig {
  private String collectionName;

  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private String timeColumnName;
  private String timeUnit;
  private String aggregationGranularity;
  private int numTimeBuckets;

  public StarTreeBootstrapPhaseTwoConfig() {

  }

  public StarTreeBootstrapPhaseTwoConfig(String collectionName,
      List<String> dimensionNames, List<String> metricNames,
      List<String> metricTypes, String timeColumnName, String timeUnit,
      String aggregationGranularity, int numTimeBuckets) {
    super();
    this.collectionName = collectionName;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
    this.timeUnit = timeUnit;
    this.aggregationGranularity = aggregationGranularity;
    this.numTimeBuckets = numTimeBuckets;
  }



  public String getCollectionName() {
    return collectionName;
  }



  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<String> getMetricTypes() {
    return metricTypes;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public String getAggregationGranularity() {
    return aggregationGranularity;
  }

  public int getNumTimeBuckets() {
    return numTimeBuckets;
  }

  public void setNumTimeBuckets(int numTimeBuckets) {
    this.numTimeBuckets = numTimeBuckets;
  }
}
