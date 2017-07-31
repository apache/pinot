package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * MetricEntity represents an individual metric. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:metric:{id}'.
 */
public class MetricEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:metric:");

  private final long id;

  protected MetricEntity(String urn, double score, List<? extends Entity> related, long id) {
    super(urn, score, related);
    this.id = id;
  }

  public long getId() {
    return id;
  }

  @Override
  public MetricEntity withScore(double score) {
    return new MetricEntity(this.getUrn(), score, this.getRelated(), this.id);
  }

  @Override
  public MetricEntity withRelated(List<? extends Entity> related) {
    return new MetricEntity(this.getUrn(), this.getScore(), related, this.id);
  }

  public static MetricEntity fromMetric(double score, Collection<? extends Entity> related, long id) {
    return new MetricEntity(TYPE.formatURN(id), score, new ArrayList<>(related), id);
  }

  public static MetricEntity fromMetric(double score, long id) {
    return fromMetric(score, new ArrayList<Entity>(), id);
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
