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
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;


public abstract class TimeSeriesBuilderFactory {
  private static final int DEFAULT_MAX_UNIQUE_SERIES_PER_SERVER_LIMIT = 100_000;
  /**
   * Default limit for the total number of values across all series.
   */
  private static final long DEFAULT_MAX_DATA_POINTS_PER_SERVER_LIMIT = 100_000_000;

  public abstract BaseTimeSeriesBuilder newTimeSeriesBuilder(
      AggInfo aggInfo,
      String id,
      TimeBuckets timeBuckets,
      List<String> tagNames,
      Object[] tagValues);

  public int getMaxUniqueSeriesPerServerLimit() {
    return DEFAULT_MAX_UNIQUE_SERIES_PER_SERVER_LIMIT;
  }

  public long getMaxDataPointsPerServerLimit() {
    return DEFAULT_MAX_DATA_POINTS_PER_SERVER_LIMIT;
  }

  public abstract void init(PinotConfiguration pinotConfiguration);
}
