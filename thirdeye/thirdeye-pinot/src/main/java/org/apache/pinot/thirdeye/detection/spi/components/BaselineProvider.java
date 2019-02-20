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

package org.apache.pinot.thirdeye.detection.spi.components;

import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.detection.spec.AbstractSpec;
import org.apache.pinot.thirdeye.detection.spi.model.TimeSeries;


/**
 * The baseline provider to calculate predicted baseline.
 */
public interface BaselineProvider<T extends AbstractSpec> extends BaseComponent<T> {
  /**
   * Compute the baseline time series for the metric slice.
   * @return the time series contains predicted baseline.
   */
  TimeSeries computePredictedTimeSeries(MetricSlice slice);

  /**
   * Compute the baseline time series for the metric slice.
   * default implementation is to call computePredictedTimeSeries and aggregate using the aggregate function
   * @return the predicted value.
   */
  default Double computePredictedAggregates(MetricSlice slice, Series.DoubleFunction aggregateFunction){
    try {
      TimeSeries baselineTimeSeries = this.computePredictedTimeSeries(slice);
      return baselineTimeSeries.getPredictedBaseline().aggregate(aggregateFunction).getDouble(0);
    } catch (Exception e){
      return Double.NaN;
    }
  }
}
