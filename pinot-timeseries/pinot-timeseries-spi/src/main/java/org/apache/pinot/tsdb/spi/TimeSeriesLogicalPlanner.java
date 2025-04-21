/**
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
package org.apache.pinot.tsdb.spi;

import java.util.List;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


/**
 * Allows time-series query languages to implement their own logical planner. The input to this planner is a
 * {@link RangeTimeSeriesRequest} and the output is a {@link TimeSeriesLogicalPlanResult}. Put simply, this abstraction
 * takes in the query text and other parameters, and returns a logical plan which is a tree of
 * {@link BaseTimeSeriesPlanNode}. Other than the plan-tree, the planner also returns a {@link TimeBuckets} which is
 * the default TimeBuckets used by the query operators at runtime. Implementations are free to adjust them as they see
 * fit. For instance, one query language might want to extend to the left or right of the time-range based on certain
 * operators. Also, see {@link LeafTimeSeriesPlanNode#getEffectiveFilter(TimeBuckets)}.
 */
public interface TimeSeriesLogicalPlanner {
  void init(PinotConfiguration pinotConfiguration);

  TimeSeriesLogicalPlanResult plan(RangeTimeSeriesRequest request, List<TableConfig> tableConfigs);
}
