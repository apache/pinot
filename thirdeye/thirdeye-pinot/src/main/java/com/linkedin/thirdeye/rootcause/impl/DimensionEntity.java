package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * DimensionEntity represents a data dimension (a cut) across multiple metrics. It is identified
 * by a key-value pair. Note, that dimension names may require standardization across different
 * metrics.
 */
public class DimensionEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:dimension:");

  private final String name;
  private final String value;

  protected DimensionEntity(String urn, double score, String name, String value) {
    super(urn, score);
    this.name = name;
    this.value = value;
  }

  public String getName() {
    return name;
  }

  public String getValue() {
    return value;
  }

  @Override
  public DimensionEntity withScore(double score) {
    return new DimensionEntity(this.getUrn(), score, this.name, this.value);
  }

  public static DimensionEntity fromDimension(double score, String name, String value) {
    return new DimensionEntity(TYPE.formatURN(name, value), score, name, value);
  }

  public static DimensionEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 4);
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("Dimension URN must have 4 parts but has '%d'", parts.length));
    return fromDimension(score, parts[2], parts[3]);
  }
}
