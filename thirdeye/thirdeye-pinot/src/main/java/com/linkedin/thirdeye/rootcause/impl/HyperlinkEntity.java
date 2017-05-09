package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;


public class HyperlinkEntity extends Entity {
  public static final EntityType TYPE = new EntityType("http:");

  public static HyperlinkEntity fromURL(String url, double score) {
    if(!TYPE.isType(url))
      throw new IllegalArgumentException(String.format("Requires prefix '%s' but got '%s'", TYPE.getPrefix(), url));
    return new HyperlinkEntity(url, score);
  }

  private HyperlinkEntity(String urn, double score) {
    super(urn, score);
  }

  public String getUrl() {
    return this.getUrn();
  }
}
