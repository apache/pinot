package com.linkedin.thirdeye.rootcause;

public class Entity {
  final String urn;
  final double score;

  public Entity(String urn, double score) {
    this.urn = urn;
    this.score = score;
  }

  public String getUrn() {
    return urn;
  }

  public double getScore() {
    return score;
  }
}
