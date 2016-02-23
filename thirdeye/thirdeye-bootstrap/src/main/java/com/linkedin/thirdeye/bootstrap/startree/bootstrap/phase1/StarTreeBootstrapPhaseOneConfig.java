package com.linkedin.thirdeye.bootstrap.startree.bootstrap.phase1;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;

import java.util.ArrayList;
import java.util.List;

public class StarTreeBootstrapPhaseOneConfig {
  private String collectionName;

  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private String timeColumnName;
  private String timeUnit;
  private int inputTimeUnitSize;
  private String aggregationGranularity;
  private int aggregationGranularitySize;
  private int numTimeBuckets;

  public StarTreeBootstrapPhaseOneConfig() {

  }

  public StarTreeBootstrapPhaseOneConfig(String collectionName, List<String> dimensionNames,
      List<String> metricNames, List<MetricType> metricTypes, String timeColumnName,
      String timeUnit, int inputTimeUnitSize, String aggregationGranularity,
      int aggregationGranularitySize, int numTimeBuckets) {
    super();
    this.collectionName = collectionName;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
    this.timeUnit = timeUnit;
    this.inputTimeUnitSize = inputTimeUnitSize;
    this.aggregationGranularity = aggregationGranularity;
    this.aggregationGranularitySize = aggregationGranularitySize;
    this.numTimeBuckets = numTimeBuckets;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public List<String> getDimensionNames() {
    return dimensionNames;
  }

  public int getAggregationGranularitySize() {
    return aggregationGranularitySize;
  }

  public List<String> getMetricNames() {
    return metricNames;
  }

  public List<MetricType> getMetricTypes() {
    return metricTypes;
  }

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public int getInputTimeUnitSize() {
    return inputTimeUnitSize;
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

  public static StarTreeBootstrapPhaseOneConfig fromStarTreeConfig(StarTreeConfig config) {
    int numTimeBuckets = (int) config.getTime().getBucket().getUnit().convert(
        config.getTime().getRetention().getSize(), config.getTime().getRetention().getUnit());

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

    return new StarTreeBootstrapPhaseOneConfig(config.getCollection(), dimensionNames, metricNames,
        metricTypes, config.getTime().getColumnName(),
        config.getTime().getInput().getUnit().toString(), config.getTime().getInput().getSize(),
        config.getTime().getBucket().getUnit().toString(), config.getTime().getBucket().getSize(),
        numTimeBuckets);
  }
}
