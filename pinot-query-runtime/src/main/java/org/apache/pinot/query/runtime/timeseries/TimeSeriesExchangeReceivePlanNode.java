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

import java.util.List;
import java.util.concurrent.BlockingQueue;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBuilderFactory;


/// With the broker-reduce mode in Time Series Engine, this node becomes the leaf stage for the broker. In other words,
/// the plan fragment that runs in the broker will always have this node in the leaves.
public class TimeSeriesExchangeReceivePlanNode extends BaseTimeSeriesPlanNode {
  private final long _deadlineMs;
  private final AggInfo _aggInfo;
  private final TimeSeriesBuilderFactory _factory;
  private BlockingQueue<Object> _receiver;
  private int _numServersQueried;
  @Nullable
  private TimeBuckets _timeBuckets;

  public TimeSeriesExchangeReceivePlanNode(String id, long deadlineMs, @Nullable AggInfo aggInfo,
      TimeSeriesBuilderFactory factory) {
    super(id, List.of());
    _deadlineMs = deadlineMs;
    _aggInfo = aggInfo;
    _factory = factory;
  }

  /// @param timeBuckets the query's time buckets, for the empty block returned when `numExpectedBlocks` is 0
  public void init(BlockingQueue<Object> receiver, int numExpectedBlocks, @Nullable TimeBuckets timeBuckets) {
    _receiver = receiver;
    _numServersQueried = numExpectedBlocks;
    _timeBuckets = timeBuckets;
  }

  @Override
  public BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs) {
    return new TimeSeriesExchangeReceivePlanNode(_id, _deadlineMs, _aggInfo, _factory);
  }

  @Override
  public String getKlass() {
    return TimeSeriesExchangeReceivePlanNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_EXCHANGE_RECEIVE";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    return new TimeSeriesExchangeReceiveOperator(_receiver, _deadlineMs, _numServersQueried, _aggInfo, _factory,
        _timeBuckets);
  }
}
