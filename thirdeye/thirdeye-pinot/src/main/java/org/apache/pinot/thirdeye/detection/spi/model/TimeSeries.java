/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.detection.spi.model;

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.datalayer.dto.AnomalyFunctionDTO;

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Wrapper object of data frame which contains baseline, upper and lower bounds.
 */
public class TimeSeries {
  private DataFrame df;
  private String metricUrn;
  private AnomalyFunctionDTO function;

  public AnomalyFunctionDTO getFunction() {
    return function;
  }

  public void setFunction(AnomalyFunctionDTO function) {
    this.function = function;
  }

  public String getMetricUrn() {
    return metricUrn;
  }

  public void setMetricUrn(String metricUrn) {
    this.metricUrn = metricUrn;
  }

  public TimeSeries(String metricUrn) {
    this.df = new DataFrame();
    this.metricUrn = metricUrn;
  }

  public TimeSeries() {
    this.df = new DataFrame();
  }

  /**
   * Add the time stamps into the timeseries
   * @param timestamps
   */
  public void addTimeStamps(LongSeries timestamps) {
    this.df.addSeries(COL_TIME, timestamps).setIndex(COL_TIME);
  }

  /**
   * Add the predicted baseline into the timeseries
   * @param baselineValues predicted baseline values
   */
  public void addPredictedBaseline(DoubleSeries baselineValues) {
    this.df.addSeries(DataFrameUtils.COL_VALUE, baselineValues);
  }

  /**
   * Add the predicted upper bound into the timeseries
   * @param upperValues predicted upper bound.
   */
  public void addPredictedUpperBound(DoubleSeries upperValues) {
    this.df.addSeries(DataFrameUtils.COL_UPPER, upperValues);
  }

  /**
   * Add the lower bound into the timeseries
   * @param lowerValues predicted baseline values
   */
  public void addPredictedLowerBound(DoubleSeries lowerValues) {
    this.df.addSeries(DataFrameUtils.COL_LOWER, lowerValues);
  }

  public static TimeSeries fromDataFrame(DataFrame df) {
    TimeSeries ts = new TimeSeries();
    ts.df.addSeries(COL_TIME, df.get(COL_TIME)).setIndex(COL_TIME);
    ts.df.addSeries(DataFrameUtils.COL_VALUE, df.get(DataFrameUtils.COL_VALUE));
    return ts;
  }

  public DoubleSeries getPredictedBaseline() {
    return this.df.getDoubles(DataFrameUtils.COL_VALUE);
  }
  public DoubleSeries getPredictedLowerBound() {
    return this.df.getDoubles(DataFrameUtils.COL_LOWER);
  }
  public DoubleSeries getPredictedUpperBound() {
    return this.df.getDoubles(DataFrameUtils.COL_UPPER);
  }

  public boolean isEmpty() {
    return df.isEmpty();
  }

  public DataFrame getDataFrame() {
    return df;
  }

  public TimeSeries merge(TimeSeries other) {
    if (df.isEmpty()) {
      df = other.df.copy();
    } else {
      df.joinOuter(other.df, COL_TIME);
    }
    return this;
  }
}
