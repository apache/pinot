package com.linkedin.thirdeye.bootstrap.startree.generation;

import java.util.List;

public class StarTreeGenerationConfig {
  private String collectionName;
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<String> metricTypes;
  private List<String> splitOrder;
  private String timeColumnName;
  private int splitThreshold;

  public StarTreeGenerationConfig() {

  }

  /**
   * 
   * @param collectionName
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param splitOrder
   * @param timeColumnName
   * @param splitThreshold
   */
  public StarTreeGenerationConfig(String collectionName,
      List<String> dimensionNames, List<String> metricNames,
      List<String> metricTypes, List<String> splitOrder, String timeColumnName,
      int splitThreshold) {
    super();
    this.collectionName = collectionName;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.splitOrder = splitOrder;
    this.timeColumnName = timeColumnName;
    this.splitThreshold = splitThreshold;
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

  public String getCollectionName() {
    return collectionName;
  }

  public List<String> getSplitOrder() {
    return splitOrder;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public int getSplitThreshold() {
    return splitThreshold;
  }

  
}
