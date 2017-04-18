package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;


/**
 * MetricEntity represents an individual metric. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:metric:{dataset}:{name}'.
 */
public class MetricEntity extends Entity {
  public static MetricEntity fromDTO(double score, MetricConfigDTO metric, DatasetConfigDTO dataset) {
    String urn = EntityUtils.EntityType.METRIC.formatUrn("%s:%s", metric.getDataset(), metric.getName());
    return new MetricEntity(urn, score, metric, dataset);
  }

  final MetricConfigDTO metric;
  final DatasetConfigDTO dataset;

  public MetricEntity(String urn, double score, MetricConfigDTO metric, DatasetConfigDTO dataset) {
    super(urn, score);
    this.metric = metric;
    this.dataset = dataset;
  }

  public MetricConfigDTO getMetric() {
    return metric;
  }

  public DatasetConfigDTO getDataset() {
    return dataset;
  }
}
