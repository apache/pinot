package com.linkedin.thirdeye.client.pinot.summary;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.TimeGranularity;

public interface OLAPDataBaseClient {

  public abstract void setBaselineStartInclusive(DateTime dateTime);

  public abstract void setCurrentStartInclusive(DateTime dateTime);

  public abstract void setMetricName(String metricName);

  public abstract void setGroupByTimeGranularity(TimeGranularity timeGranularity);

  public abstract void setCollection(String collection);

  public abstract List<Pair<Double, Double>> getAggregatedValuesInOneDimension(String dimensionName);

  public abstract Pair<Double, Double> getTopAggregatedValues();

  public abstract List<Row> getAggregatedValuesAtLevel(Dimensions dimensions, int level);
}
