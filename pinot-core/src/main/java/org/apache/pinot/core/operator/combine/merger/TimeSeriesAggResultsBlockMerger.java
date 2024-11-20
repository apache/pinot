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
package org.apache.pinot.core.operator.combine.merger;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.operator.blocks.TimeSeriesBuilderBlock;
import org.apache.pinot.core.operator.blocks.results.TimeSeriesResultsBlock;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


public class TimeSeriesAggResultsBlockMerger implements ResultsBlockMerger<TimeSeriesResultsBlock> {
  private final TimeSeriesBuilderFactory _seriesBuilderFactory;
  private final AggInfo _aggInfo;
  private final int _maxSeriesLimit;
  private final long _maxDataPointsLimit;

  public TimeSeriesAggResultsBlockMerger(TimeSeriesBuilderFactory seriesBuilderFactory, AggInfo aggInfo) {
    _seriesBuilderFactory = seriesBuilderFactory;
    _aggInfo = aggInfo;
    _maxSeriesLimit = _seriesBuilderFactory.getMaxUniqueSeriesPerServerLimit();
    _maxDataPointsLimit = _seriesBuilderFactory.getMaxDataPointsPerServerLimit();
  }

  @Override
  public void mergeResultsBlocks(TimeSeriesResultsBlock mergedBlock, TimeSeriesResultsBlock blockToMerge) {
    TimeSeriesBuilderBlock currentTimeSeriesBlock = mergedBlock.getTimeSeriesBuilderBlock();
    TimeSeriesBuilderBlock seriesBlockToMerge = blockToMerge.getTimeSeriesBuilderBlock();
    for (var entry : seriesBlockToMerge.getSeriesBuilderMap().entrySet()) {
      long seriesHash = entry.getKey();
      BaseTimeSeriesBuilder currentTimeSeriesBuilder = currentTimeSeriesBlock.getSeriesBuilderMap().get(seriesHash);
      BaseTimeSeriesBuilder newTimeSeriesToMerge = entry.getValue();
      if (currentTimeSeriesBuilder == null) {
        currentTimeSeriesBlock.getSeriesBuilderMap().put(seriesHash, newTimeSeriesToMerge);
        final long currentUniqueSeries = currentTimeSeriesBlock.getSeriesBuilderMap().size();
        final long numBuckets = currentTimeSeriesBlock.getTimeBuckets().getNumBuckets();
        Preconditions.checkState(currentUniqueSeries * numBuckets <= _maxDataPointsLimit,
            "Max data points limit reached in combine operator. Limit: %s. Current count: %s",
            _maxDataPointsLimit, currentUniqueSeries * numBuckets);
        Preconditions.checkState(currentUniqueSeries <= _maxSeriesLimit,
            "Max series limit reached in combine operator. Limit: %s. Current Size: %s",
            _maxSeriesLimit, currentTimeSeriesBlock.getSeriesBuilderMap().size());
      } else {
        currentTimeSeriesBuilder.mergeAlignedSeriesBuilder(newTimeSeriesToMerge);
      }
    }
  }
}
