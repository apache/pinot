package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * MetricEntity represents an individual metric. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:metric:{dataset}:{name}'.
 */
public class MetricEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:metric:");

  private final String dataset;
  private final String metric;

  protected MetricEntity(String urn, double score, String dataset, String metric) {
    super(urn, score);
    this.dataset = dataset;
    this.metric = metric;
  }

  public String getDataset() {
    return dataset;
  }

  public String getMetric() {
    return metric;
  }

  @Override
  public MetricEntity withScore(double score) {
    return new MetricEntity(this.getUrn(), score, this.dataset, this.metric);
  }

  public static MetricEntity fromMetric(double score, String dataset, String metric) {
    return new MetricEntity(TYPE.formatURN(dataset, metric), score, dataset, metric);
  }

  public static MetricEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 4);
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("URN must have 4 parts but has '%d'", parts.length));
    return fromMetric(score, parts[2], parts[3]);
  }
}
