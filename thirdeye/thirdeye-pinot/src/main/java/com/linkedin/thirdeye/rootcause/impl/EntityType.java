package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import org.apache.commons.lang.StringUtils;


/**
 * Wrapper class for URN prefix based typing of Entity.
 */
public final class EntityType {
  final String prefix;

  public static String extractPrefix(String urn) {
    String[] parts = urn.split(":");
    return parts[0] + ":" + parts[1] + ":";
  }

  public static String extractPrefix(Entity e) {
    return extractPrefix(e.getUrn());
  }

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

  public boolean isType(String urn) {
    return prefix.equals(extractPrefix(urn));
  }
}
