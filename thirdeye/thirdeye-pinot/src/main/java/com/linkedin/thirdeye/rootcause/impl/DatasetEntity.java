package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import com.linkedin.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * DatasetEntity represents a group of metrics tightly associated with each other. It typically serves
 * as a convenience to describe entity mappings common to whole groups of entities.
 * 'thirdeye:dataset:{name}'.
 */
public class DatasetEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:dataset:");

  private final String name;

  protected DatasetEntity(String urn, double score, List<? extends Entity> related, String name) {
    super(urn, score, related);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public DatasetEntity withScore(double score) {
    return new DatasetEntity(this.getUrn(), score, this.getRelated(), this.name);
  }

  @Override
  public DatasetEntity withRelated(List<? extends Entity> related) {
    return new DatasetEntity(this.getUrn(), this.getScore(), related, this.name);
  }

  public static DatasetEntity fromName(double score, String name) {
    String urn = TYPE.formatURN(name);
    return new DatasetEntity(urn, score, new ArrayList<Entity>(), name);
  }

  public static DatasetEntity fromName(double score, Collection<? extends Entity> related, String name) {
    String urn = TYPE.formatURN(name);
    return new DatasetEntity(urn, score, new ArrayList<>(related), name);
  }

  public static DatasetEntity fromURN(String urn, double score) {
    ParsedUrn parsedUrn = EntityUtils.parseUrnString(urn, TYPE);
    parsedUrn.assertPrefixOnly();

    String dataset = parsedUrn.getPrefixes().get(2);
    return new DatasetEntity(urn, score, Collections.<Entity>emptyList(), dataset);
  }
}
