package com.linkedin.thirdeye.rootcause;

public class Entity {
  public static Entity fromURN(String urn) {
    return new Entity(urn);
  }

  final String urn;

  protected Entity(String urn) {
    this.urn = urn;
  }

  public String getUrn() {
    return urn;
  }
}
