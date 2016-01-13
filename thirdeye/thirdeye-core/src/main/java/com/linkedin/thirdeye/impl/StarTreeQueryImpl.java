package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeQuery;
import com.linkedin.thirdeye.api.TimeRange;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class StarTreeQueryImpl implements StarTreeQuery {
  private final StarTreeConfig config;
  private final DimensionKey dimensionKey;
  private final TimeRange timeRange;

  public StarTreeQueryImpl(StarTreeConfig config, DimensionKey dimensionKey, TimeRange timeRange) {
    this.config = config;
    this.dimensionKey = dimensionKey;
    this.timeRange = timeRange;
  }

  @Override
  public Set<String> getStarDimensionNames() {
    Set<String> starDimensions = new HashSet<String>();

    for (int i = 0; i < config.getDimensions().size(); i++) {
      if (StarTreeConstants.STAR.equals(dimensionKey.getDimensionValues()[i])) {
        starDimensions.add(config.getDimensions().get(i).getName());
      }
    }

    return starDimensions;
  }

  @Override
  public DimensionKey getDimensionKey() {
    return dimensionKey;
  }

  @Override
  public TimeRange getTimeRange() {
    return timeRange;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    sb.append("dimensionKey=").append(dimensionKey);

    if (timeRange != null) {
      sb.append("timeRange=").append(timeRange);
    }

    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeQuery)) {
      return false;
    }
    StarTreeQuery q = (StarTreeQuery) o;

    if (timeRange != null) {
      return dimensionKey.equals(q.getDimensionKey()) && timeRange.equals(q.getTimeRange());
    } else {
      return q.getDimensionKey().equals(dimensionKey);
    }
  }

  @Override
  public int hashCode() {
    return dimensionKey.hashCode();
  }

  public static class Builder {
    private DimensionKey dimensionKey;
    private TimeRange timeRange;

    public DimensionKey getDimensionKey() {
      return dimensionKey;
    }

    public Builder setDimensionKey(DimensionKey dimensionKey) {
      this.dimensionKey = dimensionKey;
      return this;
    }

    public TimeRange getTimeRange() {
      return timeRange;
    }

    public Builder setTimeRange(TimeRange timeRange) {
      this.timeRange = timeRange;
      return this;
    }

    public StarTreeQuery build(StarTreeConfig config) {
      return new StarTreeQueryImpl(config, dimensionKey, timeRange);
    }
  }
}
