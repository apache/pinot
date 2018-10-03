package com.linkedin.thirdeye.rootcause.callgraph;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.impl.EntityType;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import java.util.Collections;
import java.util.List;


public class CallGraphEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:callgraph:");

  private final DataFrame edge;

  protected CallGraphEntity(String urn, double score, List<? extends Entity> related, DataFrame edge) {
    super(urn, score, related);
    this.edge = edge;
  }

  public DataFrame getEdge() {
    return new DataFrame(this.edge);
  }

  @Override
  public CallGraphEntity withScore(double score) {
    return new CallGraphEntity(this.getUrn(), score, this.getRelated(), this.getEdge());
  }

  @Override
  public CallGraphEntity withRelated(List<? extends Entity> related) {
    return new CallGraphEntity(this.getUrn(), this.getScore(), related, this.getEdge());
  }

  public CallGraphEntity withEdge(DataFrame edge) {
    return new CallGraphEntity(this.getUrn(), this.getScore(), this.getRelated(), new DataFrame(edge));
  }

  public static CallGraphEntity fromEdge(double score, DataFrame edge) {
    return fromEdge(score, Collections.<Entity>emptyList(), edge);
  }

  public static CallGraphEntity fromEdge(double score, List<? extends Entity> related, DataFrame edge) {
    if (edge.size() != 1) {
      throw new IllegalArgumentException("Must provide a data frame with exactly one row");
    }

    Multimap<String, String> dimensions = ArrayListMultimap.create();
    for (String seriesName : edge.getSeriesNames()) {
      dimensions.put(seriesName, edge.getString(seriesName, 0));
    }

    return new CallGraphEntity(TYPE.formatURN(EntityUtils.encodeDimensions(dimensions)), score, related, edge);
  }
}
