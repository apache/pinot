package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeRecord;

import java.util.Arrays;
import java.util.Collection;

public class StarTreeRecordImpl implements StarTreeRecord {
  private final StarTreeConfig config;
  private final DimensionKey dimensionKey;
  private final MetricTimeSeries metricTimeSeries;

  public StarTreeRecordImpl(StarTreeConfig config, DimensionKey dimensionKey,
      MetricTimeSeries metricTimeSeries) {
    this.config = config;
    this.dimensionKey = dimensionKey;
    this.metricTimeSeries = metricTimeSeries;
  }

  @Override
  public StarTreeConfig getConfig() {
    return config;
  }

  @Override
  public DimensionKey getDimensionKey() {
    return dimensionKey;
  }

  @Override
  public MetricTimeSeries getMetricTimeSeries() {
    return metricTimeSeries;
  }

  @Override
  public StarTreeRecord relax(String dimensionName) {
    return relax(Arrays.asList(dimensionName));
  }

  @Override
  public StarTreeRecord relax(Collection<String> dimensionNames) {
    return alias(dimensionNames, StarTreeConstants.STAR);
  }

  @Override
  public StarTreeRecord aliasOther(String dimensionName) {
    return aliasOther(Arrays.asList(dimensionName));
  }

  @Override
  public StarTreeRecord aliasOther(Collection<String> otherDimensionNames) {
    return alias(otherDimensionNames, StarTreeConstants.OTHER);
  }

  private StarTreeRecord alias(Collection<String> dimensionNames, String aliasValue) {
    String[] dimensionValues = new String[config.getDimensions().size()];

    for (int i = 0; i < config.getDimensions().size(); i++) {
      dimensionValues[i] = dimensionNames.contains(config.getDimensions().get(i).getName())
          ? aliasValue : dimensionKey.getDimensionValues()[i];
    }
    DimensionKey newDimensionKey = new DimensionKey(dimensionValues);

    MetricTimeSeries newTimeSeries = new MetricTimeSeries(metricTimeSeries.getSchema());
    newTimeSeries.aggregate(metricTimeSeries);

    return new StarTreeRecordImpl(config, newDimensionKey, newTimeSeries);
  }

  @Override
  public int hashCode() {
    return dimensionKey.hashCode() + 13 * metricTimeSeries.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeRecord)) {
      return false;
    }
    StarTreeRecord r = (StarTreeRecord) o;
    return dimensionKey.equals(r.getDimensionKey())
        && metricTimeSeries.equals(r.getMetricTimeSeries());
  }

  @Override
  public String toString() {
    return new StringBuilder().append(dimensionKey).append("\n").append(metricTimeSeries)
        .toString();
  }

  public static class Builder {
    private DimensionKey dimensionKey;
    private MetricTimeSeries metricTimeSeries;

    public DimensionKey getDimensionKey() {
      return dimensionKey;
    }

    public Builder setDimensionKey(DimensionKey dimensionKey) {
      this.dimensionKey = dimensionKey;
      return this;
    }

    public Builder updateDimensionKey(DimensionKey updateKey) {
      if (dimensionKey == null) {
        dimensionKey = updateKey;
      } else {
        for (int i = 0; i < updateKey.getDimensionValues().length; i++) {
          if (!dimensionKey.getDimensionValues()[i].equals(updateKey.getDimensionValues()[i])) {
            dimensionKey.getDimensionValues()[i] = StarTreeConstants.STAR;
          }
        }
      }
      return this;
    }

    public MetricTimeSeries getMetricTimeSeries() {
      return metricTimeSeries;
    }

    public Builder setMetricTimeSeries(MetricTimeSeries metricTimeSeries) {
      this.metricTimeSeries = metricTimeSeries;
      return this;
    }

    public Builder updateMetricTimeSeries(MetricTimeSeries timeSeries) {
      this.metricTimeSeries.aggregate(timeSeries);
      return this;
    }

    public StarTreeRecord build(StarTreeConfig config) {
      return new StarTreeRecordImpl(config, dimensionKey, metricTimeSeries);
    }

    public void clear() {
      this.dimensionKey = null;
      this.metricTimeSeries = null;
    }
  }
}
