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
package org.apache.pinot.tsdb.planner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.tsdb.spi.AggInfo;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


/**
 * This node exists in the logical plan, but not in the physical/dispatchable plans. Similar to the
 * {@link LeafTimeSeriesPlanNode}, a physical plan visitor will convert to its equivalent physical plan node, which will
 * be capable of returning an executable operator with the {@link #run()} method.
 * <br />
 * <b>Note:</b> This node doesn't exist in the pinot-timeseries-spi because we don't want to let language developers
 *   control how and when exchange will be run (as of now).
 */
public class TimeSeriesExchangeNode extends BaseTimeSeriesPlanNode {
  @Nullable
  private final AggInfo _aggInfo;

  @JsonCreator
  public TimeSeriesExchangeNode(@JsonProperty("id") String id,
      @JsonProperty("inputs") List<BaseTimeSeriesPlanNode> inputs,
      @Nullable @JsonProperty("aggInfo") AggInfo aggInfo) {
    super(id, inputs);
    _aggInfo = aggInfo;
  }

  @Nullable
  public AggInfo getAggInfo() {
    return _aggInfo;
  }

  @Override
  public BaseTimeSeriesPlanNode withInputs(List<BaseTimeSeriesPlanNode> newInputs) {
    return new TimeSeriesExchangeNode(_id, newInputs, _aggInfo);
  }

  @Override
  public String getKlass() {
    return TimeSeriesExchangeNode.class.getName();
  }

  @Override
  public String getExplainName() {
    return "TIME_SERIES_BROKER_RECEIVE";
  }

  @Override
  public BaseTimeSeriesOperator run() {
    throw new IllegalStateException("Time Series Exchange should have been replaced with a physical plan node");
  }
}
