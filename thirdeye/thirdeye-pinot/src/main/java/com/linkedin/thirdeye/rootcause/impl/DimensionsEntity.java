package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * DimensionsEntity represents a dimension cut of the data without being bound
 * to a specific entity, such as a metric. The the DimensionsEntity holds (uri encoded)
 * tuples of keys and values, e.g. 'thirdeye:dimensions:key1=value1:key2=value2'.
 *
 * <br/><b>NOTE:</b> it is the successor of DimensionEntity
 *
 * @see DimensionEntity
 */
public class DimensionsEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:dimensions:");

  private final Multimap<String, String> dimensions;
  private final String issueType;

  private DimensionsEntity(String urn, double score, List<? extends Entity> related,
      Multimap<String, String> dimensions, String issueType) {
    super(urn, score, related);
    this.dimensions = dimensions;
    this.issueType = issueType;
  }

  public Multimap<String, String> getDimensions() {
    return dimensions;
  }

  public String getIssueType() {
    return issueType;
  }

  @Override
  public DimensionsEntity withScore(double score) {
    return fromDimensions(score, this.getRelated(), this.dimensions, "");
  }

  @Override
  public DimensionsEntity withRelated(List<? extends Entity> related) {
    return fromDimensions(this.getScore(), related, this.dimensions, "");
  }

  public DimensionsEntity withDimensions(Multimap<String, String> dimensions) {
    return fromDimensions(this.getScore(), this.getRelated(), dimensions, "");
  }

  public DimensionsEntity withIssueType(String issueType) {
    return fromDimensions(this.getScore(), this.getRelated(), this.getDimensions(), issueType);
  }

  public static DimensionsEntity fromIssueType(double score, String issueType) {
    return fromDimensions(score, Collections.<Entity>emptyList(), ArrayListMultimap.<String, String>create(), issueType);
  }

  public static DimensionsEntity fromDimensions(double score, Multimap<String, String> dimensions) {
    return fromDimensions(score, Collections.<Entity>emptyList(), dimensions, "");
  }

  public static DimensionsEntity fromDimensions(double score, Collection<? extends Entity> related, Multimap<String, String> dimensions, String issueType) {
    return new DimensionsEntity(TYPE.formatURN(EntityUtils.encodeDimensions(dimensions)), score, new ArrayList<>(related), dimensions, issueType);
  }

  public static DimensionsEntity fromURN(String urn, double score) {
    return fromDimensions(score, EntityUtils.parseUrnString(urn, TYPE, 2).toFilters());
  }
}
