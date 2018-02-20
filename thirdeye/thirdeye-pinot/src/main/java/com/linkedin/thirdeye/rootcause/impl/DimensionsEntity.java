package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.rootcause.Entity;
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

  private DimensionsEntity(String urn, double score, List<? extends Entity> related,
      Multimap<String, String> dimensions) {
    super(urn, score, related);
    this.dimensions = dimensions;
  }

  public Multimap<String, String> getDimensions() {
    return dimensions;
  }

  @Override
  public DimensionsEntity withScore(double score) {
    return fromDimensions(score, this.getRelated(), this.dimensions);
  }

  @Override
  public DimensionsEntity withRelated(List<? extends Entity> related) {
    return fromDimensions(this.getScore(), related, this.dimensions);
  }

  public DimensionsEntity withDimensions(Multimap<String, String> dimensions) {
    return fromDimensions(this.getScore(), this.getRelated(), dimensions);
  }

  public static DimensionsEntity fromDimensions(double score, Multimap<String, String> dimensions) {
    return fromDimensions(score, Collections.<Entity>emptyList(), dimensions);
  }

  public static DimensionsEntity fromDimensions(double score, Collection<? extends Entity> related, Multimap<String, String> dimensions) {
    return new DimensionsEntity(TYPE.formatURN(EntityUtils.encodeDimensions(dimensions)), score, new ArrayList<>(related), dimensions);
  }

  public static DimensionsEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    // TODO handle filter strings containing ":"
    String[] parts = urn.split(":");
    if(parts.length <= 1)
      throw new IllegalArgumentException(String.format("URN must have at least 2 parts but has '%d'", parts.length));
    List<String> filterStrings = Arrays.asList(Arrays.copyOfRange(parts, 2, parts.length));
    return fromDimensions(score, EntityUtils.decodeDimensions(filterStrings));
  }
}
