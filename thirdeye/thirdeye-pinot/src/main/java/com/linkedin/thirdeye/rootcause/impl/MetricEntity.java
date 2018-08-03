package com.linkedin.thirdeye.rootcause.impl;

import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import com.linkedin.thirdeye.rootcause.util.ParsedUrn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


/**
 * MetricEntity represents an individual metric. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:metric:{id}'.
 */
public class MetricEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:metric:");

  private final long id;
  private final Multimap<String, String> filters;

  protected MetricEntity(String urn, double score, List<? extends Entity> related, long id, Multimap<String, String> filters) {
    super(urn, score, related);
    this.id = id;
    this.filters = filters;
  }

  public long getId() {
    return id;
  }

  public Multimap<String, String> getFilters() {
    return this.filters;
  }

  @Override
  public MetricEntity withScore(double score) {
    return new MetricEntity(this.getUrn(), score, this.getRelated(), this.id, this.filters);
  }

  @Override
  public MetricEntity withRelated(List<? extends Entity> related) {
    return new MetricEntity(this.getUrn(), this.getScore(), related, this.id, this.filters);
  }

  public MetricEntity withFilters(Multimap<String, String> filters) {
    return new MetricEntity(TYPE.formatURN(this.id, EntityUtils.encodeDimensions(filters)), this.getScore(), this.getRelated(), this.id, filters);
  }

  public MetricEntity withoutFilters() {
    return new MetricEntity(TYPE.formatURN(this.id), this.getScore(), this.getRelated(), this.id, filters);
  }

  public static MetricEntity fromMetric(double score, Collection<? extends Entity> related, long id, Multimap<String, String> filters) {
    return new MetricEntity(TYPE.formatURN(id, EntityUtils.encodeDimensions(filters)), score, new ArrayList<>(related), id, TreeMultimap.create(filters));
  }

  public static MetricEntity fromMetric(double score, Collection<? extends Entity> related, long id) {
    return fromMetric(score, related, id, TreeMultimap.<String, String>create());
  }

  public static MetricEntity fromMetric(double score, long id, Multimap<String, String> filters) {
    return fromMetric(score, new ArrayList<Entity>(), id, filters);
  }

  public static MetricEntity fromMetric(double score, long id) {
    return fromMetric(score, new ArrayList<Entity>(), id, TreeMultimap.<String, String>create());
  }

  public static MetricEntity fromURN(String urn, double score) {
    ParsedUrn parsedUrn = EntityUtils.parseUrnString(urn, TYPE, 3);
    long id = Long.parseLong(parsedUrn.getPrefixes().get(2));
    return new MetricEntity(urn, score, Collections.<Entity>emptyList(), id, parsedUrn.toFilters());
  }

  public static MetricEntity fromSlice(MetricSlice slice, double score) {
    return fromMetric(score, slice.getMetricId(), slice.getFilters());
  }
}
