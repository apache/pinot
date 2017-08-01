package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


public class HyperlinkEntity extends Entity {
  public static final EntityType TYPE = new EntityType("http:");

  private HyperlinkEntity(String urn, double score, List<? extends Entity> related) {
    super(urn, score, related);
  }

  public String getUrl() {
    return this.getUrn();
  }

  @Override
  public HyperlinkEntity withScore(double score) {
    return new HyperlinkEntity(this.getUrn(), score, this.getRelated());
  }

  @Override
  public HyperlinkEntity withRelated(List<? extends Entity> related) {
    return new HyperlinkEntity(this.getUrn(), this.getScore(), related);
  }

  public static HyperlinkEntity fromURL(String url, double score) {
    if(!TYPE.isType(url))
      throw new IllegalArgumentException(String.format("Requires prefix '%s' but got '%s'", TYPE.getPrefix(), url));
    return new HyperlinkEntity(url, score, new ArrayList<Entity>());
  }
}
