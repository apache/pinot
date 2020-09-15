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

package org.apache.pinot.thirdeye.datasource.loader;

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;


/**
 * Loader for metric series based on slice and an (optinal) granularity. Must be thread safe.
 */
public interface TimeSeriesLoader {
  String COL_TIME = DataFrame.COL_TIME;
  String COL_VALUE = DataFrame.COL_VALUE;

  /**
   * Returns the metric time series for a given time range and filter set, with a specified
   * time granularity. If the underlying time series resolution does not correspond to the desired
   * time granularity, it is up-sampled (via forward fill) or down-sampled (via sum if additive, or
   * last value otherwise) transparently.
   *
   * <br/><b>NOTE:</b> if the start timestamp does not align with the time
   * resolution, it is aligned with the nearest lower time stamp.
   *
   * @param slice metric slice to fetch
   * @return dataframe with aligned timestamps and values
   */
  DataFrame load(MetricSlice slice) throws Exception;
}
