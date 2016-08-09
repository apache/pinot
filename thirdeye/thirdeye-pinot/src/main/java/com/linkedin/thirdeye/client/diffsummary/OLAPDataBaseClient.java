package com.linkedin.thirdeye.client.diffsummary;

import java.util.List;

import org.joda.time.DateTime;

import com.linkedin.thirdeye.api.TimeGranularity;

public interface OLAPDataBaseClient {

  public abstract void setBaselineStartInclusive(DateTime dateTime);

  public abstract void setCurrentStartInclusive(DateTime dateTime);

  public abstract void setMetricName(String metricName);

  public abstract void setGroupByTimeGranularity(TimeGranularity timeGranularity);

  public abstract void setCollection(String collection);

  public abstract Row getTopAggregatedValues();

  public abstract List<List<Row>> getAggregatedValuesOfDimension(Dimensions dimensions);

  public abstract List<List<Row>> getAggregatedValuesOfLevels(Dimensions dimensions);

}
