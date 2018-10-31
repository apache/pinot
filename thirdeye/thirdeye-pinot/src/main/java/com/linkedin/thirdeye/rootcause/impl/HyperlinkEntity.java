package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import com.linkedin.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Collections;
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
    ParsedUrn parsedUrn = EntityUtils.parseUrnString(url, TYPE);
    parsedUrn.assertPrefixOnly();
    return new HyperlinkEntity(url, score, Collections.<Entity>emptyList());
  }
}
