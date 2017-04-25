package com.linkedin.thirdeye.rootcause.impl;

public class StringMapping {
  final String from;
  final String to;
  final double weight;

  public StringMapping(String from, String to, double weight) {
    this.from = from;
    this.to = to;
    this.weight = weight;
  }

  public String getFrom() {
    return from;
  }

  public String getTo() {
    return to;
  }

  public double getWeight() {
    return weight;
  }
}
