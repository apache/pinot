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
package org.apache.pinot.query.runtime.timeseries;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.series.BaseTimeSeriesBuilder;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


/**
 * <h2>Overview</h2>
 * Receives and optionally aggregates the response from all servers for the corresponding plan node.
 *
 * <h3>Aggregate Receive</h3>
 * When a non-null {@link AggInfo} is passed, this operator will aggregate the received data using the corresponding
 * series builder created via {@link TimeSeriesBuilderFactory}.
 *
 * <h3>Non-Aggregate Receive</h3>
 * When a null AggInfo is passed, then we don't perform any aggregation. If we receive series with the same ID from
 * different servers, we will simply append them to the list, creating a union.
 */
public class TimeSeriesExchangeReceiveOperator extends BaseTimeSeriesOperator {
  /**
   * Receiver will receive either TimeSeriesBlock or Throwable. And will have at most _numServersQueried objects that
   * can be polled.
   */
  private final BlockingQueue<Object> _receiver;
  private final long _deadlineMs;
  private final int _numServersQueried;
  @Nullable
  private final AggInfo _aggInfo;
  private final TimeSeriesBuilderFactory _factory;

  public TimeSeriesExchangeReceiveOperator(BlockingQueue<Object> receiver, long deadlineMs, int numServersQueried,
      @Nullable AggInfo aggInfo, TimeSeriesBuilderFactory seriesBuilderFactory) {
    super(Collections.emptyList());
    Preconditions.checkArgument(numServersQueried > 0, "No servers to query in receive operator");
    _receiver = receiver;
    _deadlineMs = deadlineMs;
    _numServersQueried = numServersQueried;
    _aggInfo = aggInfo;
    _factory = seriesBuilderFactory;
  }

  @Override
  public TimeSeriesBlock getNextBlock() {
    try {
      if (_aggInfo == null) {
        return getNextBlockNoAggregation();
      } else {
        return getNextBlockWithAggregation();
      }
    } catch (Throwable t) {
      throw new RuntimeException(t.getMessage(), t);
    }
  }

  private TimeSeriesBlock getNextBlockWithAggregation()
      throws Throwable {
    TimeBuckets timeBuckets = null;
    Map<Long, BaseTimeSeriesBuilder> seriesBuilderMap = new HashMap<>();
    for (int index = 0; index < _numServersQueried; index++) {
      // Step-1: Poll, and ensure we received a TimeSeriesBlock.
      long remainingTimeMs = _deadlineMs - System.currentTimeMillis();
      Preconditions.checkState(remainingTimeMs > 0,
          "Timed out before polling all servers. Successfully Polled: %s of %s", index, _numServersQueried);
      Object result = _receiver.poll(remainingTimeMs, TimeUnit.MILLISECONDS);
      Preconditions.checkNotNull(result, "Timed out waiting for response. Waited: %s ms", remainingTimeMs);
      if (result instanceof Throwable) {
        throw (Throwable) result;
      }
      Preconditions.checkState(result instanceof TimeSeriesBlock,
          "Found unexpected object. This is a bug: %s", result.getClass());
      // Step-2: Init timeBuckets and ensure they are the same across all servers.
      TimeSeriesBlock blockToMerge = (TimeSeriesBlock) result;
      if (timeBuckets == null) {
        timeBuckets = blockToMerge.getTimeBuckets();
      } else {
        Preconditions.checkState(timeBuckets.equals(blockToMerge.getTimeBuckets()),
            "Found unequal time buckets from server response");
      }
      // Step-3: Merge new block with existing block.
      for (var entry : blockToMerge.getSeriesMap().entrySet()) {
        long seriesHash = entry.getKey();
        List<TimeSeries> currentSeriesList = entry.getValue();
        TimeSeries sampledTimeSeries = currentSeriesList.get(0);
        // Init seriesBuilder if required
        BaseTimeSeriesBuilder seriesBuilder = seriesBuilderMap.get(seriesHash);
        if (seriesBuilder == null) {
          seriesBuilder = _factory.newTimeSeriesBuilder(
              _aggInfo, Long.toString(seriesHash), timeBuckets, sampledTimeSeries.getTagNames(),
              sampledTimeSeries.getTagValues());
          seriesBuilderMap.put(seriesHash, seriesBuilder);
        }
        for (TimeSeries timeSeries : currentSeriesList) {
          seriesBuilder.mergeAlignedSeries(timeSeries);
        }
      }
    }
    // Convert series builders to series and return.
    Map<Long, List<TimeSeries>> seriesMap = new HashMap<>(seriesBuilderMap.size());
    for (var entry : seriesBuilderMap.entrySet()) {
      long seriesHash = entry.getKey();
      List<TimeSeries> timeSeriesList = new ArrayList<>();
      timeSeriesList.add(entry.getValue().build());
      seriesMap.put(seriesHash, timeSeriesList);
    }
    return new TimeSeriesBlock(timeBuckets, seriesMap);
  }

  private TimeSeriesBlock getNextBlockNoAggregation()
      throws Throwable {
    Map<Long, List<TimeSeries>> timeSeriesMap = new HashMap<>();
    TimeBuckets timeBuckets = null;
    for (int index = 0; index < _numServersQueried; index++) {
      long remainingTimeMs = _deadlineMs - System.currentTimeMillis();
      Preconditions.checkState(remainingTimeMs > 0, "Timed out before polling exchange receive");
      Object result = _receiver.poll(remainingTimeMs, TimeUnit.MILLISECONDS);
      Preconditions.checkNotNull(result, "Timed out waiting for response. Waited: %s ms", remainingTimeMs);
      if (result instanceof Throwable) {
        throw ((Throwable) result);
      }
      Preconditions.checkState(result instanceof TimeSeriesBlock,
          "Found unexpected object. This is a bug: %s", result.getClass());
      TimeSeriesBlock blockToMerge = (TimeSeriesBlock) result;
      if (timeBuckets == null) {
        timeBuckets = blockToMerge.getTimeBuckets();
      } else {
        Preconditions.checkState(timeBuckets.equals(blockToMerge.getTimeBuckets()),
            "Found unequal time buckets from server response");
      }
      for (var entry : blockToMerge.getSeriesMap().entrySet()) {
        long seriesHash = entry.getKey();
        List<TimeSeries> timeSeriesList = new ArrayList<>(entry.getValue());
        timeSeriesMap.computeIfAbsent(seriesHash, (x) -> new ArrayList<>()).addAll(timeSeriesList);
      }
    }
    Preconditions.checkNotNull(timeBuckets, "Time buckets is null in exchange receive operator");
    return new TimeSeriesBlock(timeBuckets, timeSeriesMap);
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_EXCHANGE_RECEIVE";
  }
}
