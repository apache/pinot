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
import java.util.List;


public interface AggregationLoader {
  String COL_DIMENSION_NAME = "dimName";
  String COL_DIMENSION_VALUE = "dimValue";
  String COL_VALUE = DataFrame.COL_VALUE;

  /**
   * Returns a de-aggregation data frame for a given slice with 3 columns:
   * dimension name, dimension value, and metric value.
   *
   * @param slice metric slice
   * @param limit top k element limit per dimension name ({@code -1} for default)
   * @return de-aggregation data frame
   * @throws Exception
   */
  DataFrame loadBreakdown(MetricSlice slice, int limit) throws Exception;

  /**
   * Returns metric aggregates grouped by the given dimensions (or none).
   *
   * @param slice metric slice
   * @param dimensions dimension names to group by
   * @param limit top k element limit ({@code -1} for default)
   * @return aggregates data frame
   * @throws Exception
   */
  DataFrame loadAggregate(MetricSlice slice, List<String> dimensions, int limit) throws Exception;
}
