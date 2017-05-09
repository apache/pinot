package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import org.apache.commons.lang.StringUtils;


/**
 * Wrapper class for URN prefix based typing of Entity.
 */
public final class EntityType {
  private final String prefix;

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
    return urn.startsWith(this.prefix);
  }

  public boolean isType(Entity e) {
    return e.getUrn().startsWith(this.prefix);
  }
}
