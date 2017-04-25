package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


public class ServiceEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:service:");

  public static ServiceEntity fromName(double score, String name) {
    String urn = TYPE.formatURN(name);
    return new ServiceEntity(urn, score, name);
  }

  public static ServiceEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String name = urn.split(":")[2];
    return new ServiceEntity(urn, score, name);
  }

  final String name;

  protected ServiceEntity(String urn, double score, String name) {
    super(urn, score);
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
