package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


public class DimensionEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:dimension:");

  public static DimensionEntity fromDimension(double score, String name, String value) {
    return new DimensionEntity(TYPE.formatURN(name, value), score, name, value);
  }

  final String name;
  final String value;

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
}
