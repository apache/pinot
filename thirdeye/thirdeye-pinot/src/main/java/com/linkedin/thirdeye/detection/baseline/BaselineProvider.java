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

package com.linkedin.thirdeye.detection.baseline;

import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import com.linkedin.thirdeye.detection.DataProvider;
import java.util.Collection;
import java.util.Map;


/**
 * The baseline provider in the detection framework. Lower level interface with data provider.
 */
public interface BaselineProvider {
  /**
   * Initialize the baseline provider
   * @param properties the properties of this baseline provider.
   */
  void init(Map<String, Object> properties);

  /**
   * Fetch the baseline time series for the collection of metric slices.
   * @param slices the metric slices
   * @param provider the data source for time series, aggregates, anomalies, etc.
   * @return the mapping of the metric slice to its time series data frame.
   */
  Map<MetricSlice, DataFrame> fetchBaselineTimeSeries(Collection<MetricSlice> slices, DataProvider provider);

  /**
   * Fetch the baseline time series for the collection of metric slices.
   * @param slices the metric slices
   * @param provider the data source for time series, aggregates, anomalies, etc.
   * @return the mapping of the metric slice to its aggregate value.
   */
  Map<MetricSlice, Double> fetchBaselineAggregates(Collection<MetricSlice> slices, DataProvider provider);
}
