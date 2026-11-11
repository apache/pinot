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
package org.apache.pinot.core.segment.processing.aggregator;

import java.util.Map;


/// Value aggregator that keeps the value with the latest original time.
///
/// It relies on the caller (RollupReducer) invoking [#aggregate] with values in ascending original time order, which
/// is guaranteed by sorting the rows on the hidden original time column
/// ([org.apache.pinot.core.segment.processing.timehandler.TimeHandler#ORIGINAL_TIME_MS_COLUMN]) within each rollup
/// group, so it simply takes the new value.
///
/// NOTE: The ordering is based on the time column values of the input segments. Within a single rollup pass the
/// ordering is exact (pre-rounding time). Across multiple passes (multi-level merges, or re-merging with late
/// arriving data), the ordering is based on the already-rounded time of the earlier pass, so it is approximate at
/// the granularity of the previous round bucket. Rows with identical original time values are ordered arbitrarily
/// (the sort is not stable), so the pick among exact ties is non-deterministic, including across task retries.
public class LastWithTimeValueAggregator implements ValueAggregator {

  @Override
  public Object aggregate(Object value1, Object value2, Map<String, String> functionParameters) {
    return value2;
  }
}
