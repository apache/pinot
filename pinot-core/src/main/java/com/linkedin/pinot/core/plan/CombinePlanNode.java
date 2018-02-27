/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.plan;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MCombineGroupByOperator;
import com.linkedin.pinot.core.operator.MCombineOperator;
import com.linkedin.pinot.core.query.exception.BadQueryRequestException;
import com.linkedin.pinot.core.util.trace.TraceCallable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombinePlanNode</code> class provides the execution plan for combining results from multiple segments.
 */
public class CombinePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinePlanNode.class);

  private static final int NUM_PLAN_NODES_THRESHOLD_FOR_PARALLEL_RUN = 10;
  private static final int TIME_OUT_IN_MILLISECONDS_FOR_PARALLEL_RUN = 10_000;

  private final List<PlanNode> _planNodes;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;

  /**
   * Constructor for the class.
   *
   * @param planNodes List of underlying plan nodes
   * @param brokerRequest Broker request
   * @param executorService Executor service
   * @param timeOutMs Time out in milliseconds for query execution (not for planning phase)
   */
  public CombinePlanNode(List<PlanNode> planNodes, BrokerRequest brokerRequest, ExecutorService executorService,
      long timeOutMs) {
    _planNodes = planNodes;
    _brokerRequest = brokerRequest;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
  }

  @Override
  public Operator run() {
    int numPlanNodes = _planNodes.size();
    List<Operator> operators = new ArrayList<>(numPlanNodes);

    if (numPlanNodes < NUM_PLAN_NODES_THRESHOLD_FOR_PARALLEL_RUN) {
      // Small number of plan nodes, run them sequentially
      for (PlanNode planNode : _planNodes) {
        operators.add(planNode.run());
      }
    } else {
      // Large number of plan nodes, run them in parallel

      // Calculate the time out timestamp
      long endTime = System.currentTimeMillis() + TIME_OUT_IN_MILLISECONDS_FOR_PARALLEL_RUN;

      // Submit all jobs
      Future[] futures = new Future[numPlanNodes];
      for (int i = 0; i < numPlanNodes; i++) {
        final int index = i;
        futures[i] = _executorService.submit(new TraceCallable<Operator>() {
          @Override
          public Operator callJob() throws Exception {
            return _planNodes.get(index).run();
          }
        });
      }

      // Get all results
      try {
        for (Future future : futures) {
          operators.add((Operator) future.get(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS));
        }
      } catch (Exception e) {
        // Future object will throw ExecutionException for execution exception, need to check the cause to determine
        // whether it is caused by bad query
        Throwable cause = e.getCause();
        if (cause instanceof BadQueryRequestException) {
          throw (BadQueryRequestException) cause;
        } else {
          throw new RuntimeException("Caught exception while running CombinePlanNode.", e);
        }
      } finally {
        // Cancel all ongoing jobs
        for (Future future : futures) {
          if (!future.isDone()) {
            future.cancel(true);
          }
        }
      }
    }

    // TODO: use the same combine operator for both aggregation and selection query.
    if (_brokerRequest.isSetAggregationsInfo() && _brokerRequest.getGroupBy() != null) {
      // Aggregation group-by query
      return new MCombineGroupByOperator(operators, _executorService, _timeOutMs, _brokerRequest);
    } else {
      // Selection or aggregation only query
      return new MCombineOperator(operators, _executorService, _timeOutMs, _brokerRequest);
    }
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Instance Level Inter-Segments Combine Plan Node:");
    LOGGER.debug(prefix + "Operator: MCombineOperator/MCombineGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: BrokerRequest - " + _brokerRequest);
    int i = 1;
    for (PlanNode planNode : _planNodes) {
      LOGGER.debug(prefix + "Argument " + (i++) + ":");
      planNode.showTree(prefix + "    ");
    }
  }
}
