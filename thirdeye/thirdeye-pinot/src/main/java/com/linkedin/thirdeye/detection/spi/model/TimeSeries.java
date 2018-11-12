/*
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection.spi.model;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.util.DataFrameUtils;

/**
 * Time series. wrapper object of data frame. Used by baselineProvider to return the predicted time series
 */
public class TimeSeries {
  private DataFrame df;

  public TimeSeries() {
    this.df = new DataFrame();
  }

  /**
   * Add the time stamps into the timeseries
   * @param timestamps
   */
  public void addTimeStamps(LongSeries timestamps) {
    this.df.addSeries(DataFrameUtils.COL_TIME, timestamps);
  }

  /**
   * Add the predicted baseline into the timeseries
   * @param baselineValues predicted baseline values
   */
  public void addPredictedBaseline(DoubleSeries baselineValues) {
    this.df.addSeries(DataFrameUtils.COL_VALUE, baselineValues);
  }

  public static TimeSeries fromDataFrame(DataFrame df) {
    TimeSeries ts = new TimeSeries();
    ts.df.addSeries(DataFrameUtils.COL_TIME, df.get(DataFrameUtils.COL_TIME));
    ts.df.addSeries(DataFrameUtils.COL_VALUE, df.get(DataFrameUtils.COL_VALUE));
    return ts;
  }

  public DoubleSeries getPredictedBaseline() {
    return this.df.getDoubles(DataFrameUtils.COL_VALUE);
  }

  public DataFrame getDataFrame() {
    return df;
  }
}
