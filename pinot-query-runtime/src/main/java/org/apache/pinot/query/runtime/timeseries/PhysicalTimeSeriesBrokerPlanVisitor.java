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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import org.apache.pinot.tsdb.planner.TimeSeriesExchangeNode;
import org.apache.pinot.tsdb.spi.operator.BaseTimeSeriesOperator;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class PhysicalTimeSeriesBrokerPlanVisitor {
  // Warning: Don't use singleton access pattern, since Quickstarts run in a single JVM and spawn multiple broker/server
  public PhysicalTimeSeriesBrokerPlanVisitor() {
  }

  public void init() {
  }

  public BaseTimeSeriesOperator compile(BaseTimeSeriesPlanNode rootNode, TimeSeriesExecutionContext context,
      Map<String, Integer> numInputServersByExchangeNode) {
    // Step-1: Replace time series exchange node with its Physical Plan Node.
    rootNode = initExchangeReceivePlanNode(rootNode, context, numInputServersByExchangeNode);
    // Step-2: Trigger recursive operator generation
    return rootNode.run();
  }

  public BaseTimeSeriesPlanNode initExchangeReceivePlanNode(BaseTimeSeriesPlanNode planNode,
      TimeSeriesExecutionContext context, Map<String, Integer> numInputServersByExchangeNode) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      throw new IllegalStateException("Found leaf time series plan node in broker");
    } else if (planNode instanceof TimeSeriesExchangeNode) {
      int numInputServers = numInputServersByExchangeNode.get(planNode.getId());
      return compileToPhysicalReceiveNode((TimeSeriesExchangeNode) planNode, context, numInputServers);
    }
    List<BaseTimeSeriesPlanNode> newInputs = new ArrayList<>();
    for (int index = 0; index < planNode.getInputs().size(); index++) {
      BaseTimeSeriesPlanNode inputNode = planNode.getInputs().get(index);
      if (inputNode instanceof TimeSeriesExchangeNode) {
        int numInputServers = numInputServersByExchangeNode.get(inputNode.getId());
        TimeSeriesExchangeReceivePlanNode exchangeReceivePlanNode = compileToPhysicalReceiveNode(
            (TimeSeriesExchangeNode) inputNode, context, numInputServers);
        newInputs.add(exchangeReceivePlanNode);
      } else {
        newInputs.add(initExchangeReceivePlanNode(inputNode, context, numInputServersByExchangeNode));
      }
    }
    return planNode.withInputs(newInputs);
  }

  TimeSeriesExchangeReceivePlanNode compileToPhysicalReceiveNode(TimeSeriesExchangeNode exchangeNode,
      TimeSeriesExecutionContext context, int numServersQueried) {
    TimeSeriesExchangeReceivePlanNode exchangeReceivePlanNode = new TimeSeriesExchangeReceivePlanNode(
        exchangeNode.getId(), context.getDeadlineMs(), exchangeNode.getAggInfo(), context.getSeriesBuilderFactory());
    BlockingQueue<Object> receiver = context.getExchangeReceiverByPlanId().get(exchangeNode.getId());
    exchangeReceivePlanNode.init(Objects.requireNonNull(receiver, "No receiver for node"), numServersQueried);
    return exchangeReceivePlanNode;
  }
}
