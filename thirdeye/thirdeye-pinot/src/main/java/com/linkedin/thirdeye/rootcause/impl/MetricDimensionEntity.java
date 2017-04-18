package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;


/**
 * MetricDimensionEntity represents an single dimension of an individual metric. Holds meta-data
 * about the referenced dimension in addition to MetricEntity meta-data. The URN namespace is defined
 * as 'thirdeye:metric:{dataset}:{name}:{dimension}' and as such overlaps with MetricEntity.
 */
public class MetricDimensionEntity extends MetricEntity {
  public static MetricDimensionEntity fromDTO(double score, MetricConfigDTO dto, DatasetConfigDTO dataset, String dimension) {
    String urn = EntityType.METRIC.formatUrn("%s:%s:%s", dto.getDataset(), dto.getName(), dimension);
    return new MetricDimensionEntity(urn, score, dto, dataset, dimension);
  }

  final String dimension;

  public MetricDimensionEntity(String urn, double score, MetricConfigDTO metric, DatasetConfigDTO dataset, String dimension) {
    super(urn, score, metric, dataset);
    this.dimension = dimension;
  }

  public String getDimension() {
    return dimension;
  }

  public MetricDimensionEntity withScore(double score) {
    return new MetricDimensionEntity(this.getUrn(), score, this.getMetric(), this.getDataset(), this.getDimension());
  }

}
