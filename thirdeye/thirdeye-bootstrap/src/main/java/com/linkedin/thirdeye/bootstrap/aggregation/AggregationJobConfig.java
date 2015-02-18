package com.linkedin.thirdeye.bootstrap.aggregation;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeConfig;

public class AggregationJobConfig {
  private List<String> dimensionNames;
  private List<String> metricNames;
  private List<MetricType> metricTypes;
  private String timeColumnName;
  private String timeUnit;
  private String aggregationGranularity;

  public AggregationJobConfig() {

  }

  public AggregationJobConfig(List<String> dimensionNames,
      List<String> metricNames, List<MetricType> metricTypes,
      String timeColumnName, String timeUnit, String aggregationGranularity) {
    super();
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.timeColumnName = timeColumnName;
    this.timeUnit = timeUnit;
    this.aggregationGranularity = aggregationGranularity;
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

  public String getTimeColumnName() {
    return timeColumnName;
  }

  public String getTimeUnit() {
    return timeUnit;
  }

  public String getAggregationGranularity() {
    return aggregationGranularity;
  }

  public static AggregationJobConfig fromStarTreeConfig(StarTreeConfig config) {
    List<String> metricNames = new ArrayList<String>(config.getMetrics().size());
    List<MetricType> metricTypes = new ArrayList<MetricType>(config.getMetrics().size());
    for (MetricSpec spec : config.getMetrics())
    {
      metricNames.add(spec.getName());
      metricTypes.add(spec.getType());
    }

    List<String> dimensionNames = new ArrayList<String>(config.getDimensions().size());
    for (DimensionSpec dimensionSpec : config.getDimensions())
    {
      dimensionNames.add(dimensionSpec.getName());
    }

    return new AggregationJobConfig(dimensionNames,
                                    metricNames,
                                    metricTypes,
                                    config.getTime().getColumnName(),
                                    config.getTime().getInput().getUnit().toString(),
                                    config.getTime().getBucket().getUnit().toString());
  }
}
