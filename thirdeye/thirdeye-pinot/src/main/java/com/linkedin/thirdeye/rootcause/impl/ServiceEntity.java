package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


/**
 * ServiceEntity represents a service associated with certain metrics or dimensions. It typically
 * serves as a connecting piece between observed discrepancies between current and baseline metrics
 * and root cause events such as code deployments.
 */
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
