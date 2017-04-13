package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;


public class MetricDimensionEntity extends Entity {
  public static MetricDimensionEntity fromDTO(MetricConfigDTO metric, DatasetConfigDTO dataset, String dimension) {
    String urn = String.format("thirdeye:metricdimension:%s:%s:%s", dataset.getDataset(), metric.getName(), dimension);
    return new MetricDimensionEntity(urn, metric, dataset, dimension);
  }

  final MetricConfigDTO metric;
  final DatasetConfigDTO dataset;
  final String dimension;

  public MetricDimensionEntity(String urn, MetricConfigDTO metric, DatasetConfigDTO dataset, String dimension) {
    super(urn);
    this.metric = metric;
    this.dataset = dataset;
    this.dimension = dimension;
  }

  public MetricConfigDTO getMetric() {
    return metric;
  }

  public DatasetConfigDTO getDataset() {
    return dataset;
  }

  public String getDimension() {
    return dimension;
  }
}
