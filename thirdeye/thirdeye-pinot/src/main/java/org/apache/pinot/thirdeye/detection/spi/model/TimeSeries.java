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

import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.LongSeries;

/**
 * Time series. wrapper object of data frame. Used by baselineProvider to return the predicted time series
 */
public class TimeSeries {
  private DataFrame df;

  private TimeSeries() {
    this.df = new DataFrame();
  }

  public TimeSeries(LongSeries timestamps, DoubleSeries baselineValues) {
    this.df = new DataFrame();
    this.df.addSeries(DataFrame.COL_TIME, timestamps).setIndex(DataFrame.COL_TIME);
    this.df.addSeries(DataFrame.COL_VALUE, baselineValues);
  }

  public TimeSeries(LongSeries timestamps, DoubleSeries baselineValues, DoubleSeries currentValues,
      DoubleSeries upperBoundValues, DoubleSeries lowerBoundValues) {
    this(timestamps, baselineValues);
    this.df.addSeries(DataFrame.COL_CURRENT, currentValues);
    this.df.addSeries(DataFrame.COL_UPPER_BOUND, upperBoundValues);
    this.df.addSeries(DataFrame.COL_LOWER_BOUND, lowerBoundValues);
  }

  /**
   * the size of the time series
   * @return the size of the time series (number of data points)
   */
  public int size() {
    return this.df.size();
  }

  /**
   * Add the series into TimeSeries if it exists in the DataFrame.
   * @param df The source DataFrame.
   * @param name The series name.
   */
  private static void addSeries(TimeSeries ts, DataFrame df, String name) {
    if (df.contains(name)) {
      ts.df.addSeries(name, df.get(name));
    }
  }

  /**
   * return a empty time series
   * @return a empty time series
   */
  public static TimeSeries empty() {
    TimeSeries ts = new TimeSeries();
    ts.df.addSeries(DataFrame.COL_TIME, LongSeries.empty())
        .addSeries(DataFrame.COL_VALUE, DoubleSeries.empty())
        .addSeries(DataFrame.COL_CURRENT, DoubleSeries.empty())
        .addSeries(DataFrame.COL_UPPER_BOUND, DoubleSeries.empty())
        .addSeries(DataFrame.COL_LOWER_BOUND, DoubleSeries.empty())
        .setIndex(DataFrame.COL_TIME);
    return ts;
  }

  /**
   * Add DataFrame into TimeSeries.
   * @param df The source DataFrame.
   * @return TimeSeries that contains the predicted values.
   */
  public static TimeSeries fromDataFrame(DataFrame df) {
    Preconditions.checkArgument(df.contains(DataFrame.COL_TIME));
    Preconditions.checkArgument(df.contains(DataFrame.COL_VALUE));
    TimeSeries ts = new TimeSeries();
    // time stamp
    ts.df.addSeries(DataFrame.COL_TIME, df.get(DataFrame.COL_TIME)).setIndex(DataFrame.COL_TIME);
    // predicted baseline values
    addSeries(ts, df, DataFrame.COL_VALUE);
    // current values
    addSeries(ts, df, DataFrame.COL_CURRENT);
    // upper bound
    addSeries(ts, df, DataFrame.COL_UPPER_BOUND);
    // lower bound
    addSeries(ts, df, DataFrame.COL_LOWER_BOUND);
    return ts;
  }

  public DoubleSeries getCurrent() {
    return this.df.getDoubles(DataFrame.COL_CURRENT);
  }

  public LongSeries getTime() {
    return this.df.getLongs(DataFrame.COL_TIME);
  }

  public DoubleSeries getPredictedBaseline() {
    return this.df.getDoubles(DataFrame.COL_VALUE);
  }

  public DoubleSeries getPredictedUpperBound() {
    return this.df.getDoubles(DataFrame.COL_UPPER_BOUND);
  }

  public DoubleSeries getPredictedLowerBound() {
    return this.df.getDoubles(DataFrame.COL_LOWER_BOUND);
  }

  public DataFrame getDataFrame() {
    return df;
  }

  @Override
  public String toString() {
    return "TimeSeries{" + "df=" + df + '}';
  }
}
