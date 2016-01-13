package com.linkedin.thirdeye.bootstrap.startree.generation;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.ArrayList;
import java.util.List;

public class StarTreeGenerationConfig {
  private String collectionName;
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private List<String> splitOrder;
  private String timeColumnName;
  private int splitThreshold;

  public StarTreeGenerationConfig() {

  }

  /**
   * @param collectionName
   * @param dimensionNames
   * @param metricNames
   * @param metricTypes
   * @param splitOrder
   * @param timeColumnName
   * @param splitThreshold
   */
  public StarTreeGenerationConfig(String collectionName, List<String> dimensionNames,
      List<String> metricNames, List<MetricType> metricTypes, List<String> splitOrder,
      String timeColumnName, int splitThreshold) {
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

  public List<MetricType> getMetricTypes() {
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

  public static StarTreeGenerationConfig fromStarTreeConfig(StarTreeConfig config) {
    List<String> metricNames = new ArrayList<String>(config.getMetrics().size());
    List<MetricType> metricTypes = new ArrayList<MetricType>(config.getMetrics().size());
    for (MetricSpec spec : config.getMetrics()) {
      metricNames.add(spec.getName());
      metricTypes.add(spec.getType());
    }

    List<String> dimensionNames = new ArrayList<String>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions()) {
      dimensionNames.add(dimensionSpec.getName());
    }

    return new StarTreeGenerationConfig(config.getCollection(), dimensionNames, metricNames,
        metricTypes, config.getSplit().getOrder(), config.getTime().getColumnName(),
        config.getSplit().getThreshold());
  }
}
