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

package org.apache.pinot.thirdeye.rootcause.timeseries;

import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import java.util.List;
import java.util.Map;


/**
 * Interface for synthetic baselines constructed from one or multiple raw metric slices pointing to
 * different time periods. The offsets to chose for these time slices are determined by the implementation.
 * All implementations must support multi-indexed data frames with an index consisting of at least a
 * time column (and possibly additional columns) and a value column.
 *
 * The interface supports a scatter-gather flow of data - turning 1 base slice into 1..N data slices
 * (scatter), then filtering a N..M sized set of results down to the specifically required N slices
 * and reduce the N results down to 1 aggregate result (gather).
 */
public interface Baseline {
  String COL_TIME = DataFrame.COL_TIME;
  String COL_VALUE = DataFrame.COL_VALUE;

  /**
   * Returns the set of raw data slices required to compute the synthetic baseline for the given input slice.
   *
   * @param slice base metric slice
   * @return set of raw metric slices
   */
  List<MetricSlice> scatter(MetricSlice slice);

  /**
   * Returns the synthetic baseline computed from a set of inputs.
   * If a series is provided for slice, aligns timestamps for coarse grained data.
   *
   * <br/><b>NOTE:</b> Must filter out non-matching slices from a potentially large pool of results.
   *
   * @param slice base metric slice
   * @param data map of intermediate result dataframes (keyed by metric slice)
   * @return synthetic result dataframe
   */
  DataFrame gather(MetricSlice slice, Map<MetricSlice, DataFrame> data);
}
