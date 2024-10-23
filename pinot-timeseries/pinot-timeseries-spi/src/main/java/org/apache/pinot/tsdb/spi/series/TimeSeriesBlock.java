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
package org.apache.pinot.tsdb.spi.series;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.TimeBuckets;


/**
 * Block used by time series operators. We store the series data in a map keyed by the series' ID. The value is a
 * list of series, because some query languages support "union" operations which allow series with the same tags/labels
 * to exist either in the query response or temporarily during execution before some n-ary series function
 * is applied.
 */
public class TimeSeriesBlock {
  private final TimeBuckets _timeBuckets;
  /**
   * Refer to {@link TimeSeries} for semantics on how to compute the Long hashed value from a
   * {@link TimeSeries#getId()}.
   */
  private final Map<Long, List<TimeSeries>> _seriesMap;

  public TimeSeriesBlock(@Nullable TimeBuckets timeBuckets, Map<Long, List<TimeSeries>> seriesMap) {
    _timeBuckets = timeBuckets;
    _seriesMap = seriesMap;
  }

  @Nullable
  public TimeBuckets getTimeBuckets() {
    return _timeBuckets;
  }

  public Map<Long, List<TimeSeries>> getSeriesMap() {
    return _seriesMap;
  }
}
