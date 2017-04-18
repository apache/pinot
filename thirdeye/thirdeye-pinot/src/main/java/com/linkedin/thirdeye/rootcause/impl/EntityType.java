package com.linkedin.thirdeye.rootcause.impl;

public enum EntityType {
  EVENT("thirdeye:event"),
  METRIC("thirdeye:metric"),
  TIMERANGE("thirdeye:timerange"),
  BASELINE("thirdeye:baseline"),
  UNKNOWN("");

  private final String prefix;

  EntityType(String prefix) {
    this.prefix = prefix;
  }

  public String getPrefix() {
    return prefix;
  }

  public String formatUrn(String format, Object... values) {
    return this.prefix + ":" + String.format(format, values);
  }
}
