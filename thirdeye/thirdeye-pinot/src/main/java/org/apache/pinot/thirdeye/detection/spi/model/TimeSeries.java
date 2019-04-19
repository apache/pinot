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

import static org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils.*;


/**
 * Time series. wrapper object of data frame. Used by baselineProvider to return the predicted time series
 */
public class TimeSeries {
  private DataFrame df;

  public TimeSeries() {
    this.df = new DataFrame();
  }

  public TimeSeries(LongSeries timestamps, DoubleSeries baselineValues) {
    this.df.addSeries(COL_TIME, timestamps).setIndex(COL_TIME);
    this.df.addSeries(DataFrameUtils.COL_VALUE, baselineValues);
  }

  public TimeSeries(LongSeries timestamps, DoubleSeries baselineValues, DoubleSeries currentValues,
      DoubleSeries upperBoundValues, DoubleSeries lowerBoundValues) {
    this(timestamps, baselineValues);
    this.df.addSeries(DataFrameUtils.COL_CURRENT, currentValues);
    this.df.addSeries(DataFrameUtils.COL_UPPER_BOUND, upperBoundValues);
    this.df.addSeries(DataFrameUtils.COL_LOWER_BOUND, lowerBoundValues);
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
   * Add DataFrame into TimeSeries.
   * @param df The source DataFrame.
   * @return TimeSeries that contains the predicted values.
   */
  public static TimeSeries fromDataFrame(DataFrame df) {
    TimeSeries ts = new TimeSeries();
    ts.df.addSeries(COL_TIME, df.get(COL_TIME)).setIndex(COL_TIME);
    addSeries(ts, df, COL_VALUE);
    addSeries(ts, df, COL_CURRENT);
    addSeries(ts, df, COL_UPPER_BOUND);
    addSeries(ts, df, COL_LOWER_BOUND);
    return ts;
  }

  public DoubleSeries getCurrent() {
    return this.df.getDoubles(DataFrameUtils.COL_CURRENT);
  }

  public DoubleSeries getPredictedBaseline() {
    return this.df.getDoubles(DataFrameUtils.COL_VALUE);
  }

  public DoubleSeries getPredictedUpperBound() {
    return this.df.getDoubles(DataFrameUtils.COL_UPPER_BOUND);
  }

  public DoubleSeries getPredictedLowerBound() {
    return this.df.getDoubles(DataFrameUtils.COL_LOWER_BOUND);
  }

  public DataFrame getDataFrame() {
    return df;
  }
}
