package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import org.apache.commons.lang.StringUtils;


public final class EntityType {
  final String prefix;

  public String getPrefix() {
    return prefix;
  }

  public EntityType(String prefix) {
    if(!prefix.endsWith(":"))
      throw new IllegalArgumentException("Prefix must end with ':'");
    this.prefix = prefix;
  }

  public String formatURN(Object... values) {
    return this.prefix + StringUtils.join(values, ":");
  }

  public static String extractPrefix(String urn) {
    String[] parts = urn.split(":");
    return parts[0] + ":" + parts[1] + ":";
  }

  public static String extractPrefix(Entity e) {
    return extractPrefix(e.getUrn());
  }
}
