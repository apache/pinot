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
import org.apache.pinot.tsdb.spi.series.builders.MaxTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.MinTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.builders.SummingTimeSeriesBuilder;


public class SimpleTimeSeriesBuilderFactory extends TimeSeriesBuilderFactory {
  private final int _maxSeriesLimit;
  private final long _maxDataPointsLimit;

  public SimpleTimeSeriesBuilderFactory() {
    this(DEFAULT_MAX_UNIQUE_SERIES_PER_SERVER_LIMIT, DEFAULT_MAX_DATA_POINTS_PER_SERVER_LIMIT);
  }

  public SimpleTimeSeriesBuilderFactory(int maxSeriesLimit, long maxDataPointsLimit) {
    super();
    _maxSeriesLimit = maxSeriesLimit;
    _maxDataPointsLimit = maxDataPointsLimit;
  }

  @Override
  public BaseTimeSeriesBuilder newTimeSeriesBuilder(AggInfo aggInfo, String id, TimeBuckets timeBuckets,
      List<String> tagNames, Object[] tagValues) {
    switch (aggInfo.getAggFunction().toUpperCase()) {
      case "SUM":
        return new SummingTimeSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case "MIN":
        return new MinTimeSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      case "MAX":
        return new MaxTimeSeriesBuilder(id, timeBuckets, tagNames, tagValues);
      default:
        throw new UnsupportedOperationException("Unsupported aggregation: " + aggInfo.getAggFunction());
    }
  }

  @Override
  public void init(PinotConfiguration pinotConfiguration) {
  }

  @Override
  public int getMaxUniqueSeriesPerServerLimit() {
    return _maxSeriesLimit;
  }

  @Override
  public long getMaxDataPointsPerServerLimit() {
    return _maxDataPointsLimit;
  }
}
