package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * MetricEntity represents an individual metric. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:metric:{id}'.
 */
public class MetricEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:metric:");

  private final long id;

  protected MetricEntity(String urn, double score, long id) {
    super(urn, score);
    this.id = id;
  }

  public long getId() {
    return id;
  }

  @Override
  public MetricEntity withScore(double score) {
    return new MetricEntity(this.getUrn(), score, this.id);
  }

  public static MetricEntity fromMetric(double score, long id) {
    return new MetricEntity(TYPE.formatURN(id), score, id);
  }

  public static MetricEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 3);
    if(parts.length != 3)
      throw new IllegalArgumentException(String.format("URN must have 3 parts but has '%d'", parts.length));
    return fromMetric(score, Long.parseLong(parts[2]));
  }
}
