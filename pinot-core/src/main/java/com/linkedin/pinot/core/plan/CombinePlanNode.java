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

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.MCombineGroupByOperator;
import com.linkedin.pinot.core.operator.MCombineOperator;
import com.linkedin.pinot.core.util.trace.TraceRunnable;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombinePlanNode</code> class provides the execution plan for combining results from multiple segments.
 */
public class CombinePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinePlanNode.class);

  private static final int NUM_PLAN_NODES_THRESHOLD_FOR_PARALLEL_RUN = 10;
  private static final int TIME_OUT_IN_SECONDS_FOR_PARALLEL_RUN = 10;

  private final List<PlanNode> _planNodes;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;

  /**
   * Constructor.
   *
   * @param planNodes list of underlying plan nodes.
   * @param brokerRequest broker request.
   * @param executorService executor service.
   * @param timeOutMs time out in milliseconds.
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
    long start = System.currentTimeMillis();

    int numPlanNodes = _planNodes.size();
    List<Operator> operators = new ArrayList<>(numPlanNodes);

    if (numPlanNodes < NUM_PLAN_NODES_THRESHOLD_FOR_PARALLEL_RUN) {
      // Small number of plan nodes, run them sequentially.
      for (PlanNode planNode : _planNodes) {
        operators.add(planNode.run());
      }
    } else {
      // Large number of plan nodes, run them parallel.
      final CountDownLatch latch = new CountDownLatch(numPlanNodes);
      final Queue<Operator> globalQueue = new ConcurrentLinkedQueue<>();
      for (final PlanNode planNode : _planNodes) {
        _executorService.execute(new TraceRunnable() {
          @Override
          public void runJob() {
            try {
              Operator operator = planNode.run();
              globalQueue.add(operator);
            } catch (Exception e) {
              LOGGER.error("Caught exception while executing segment plan.", e);
            } finally {
              latch.countDown();
            }
          }
        });
      }
      try {
        latch.await(TIME_OUT_IN_SECONDS_FOR_PARALLEL_RUN, TimeUnit.SECONDS);

        // Check if all plans succeeded.
        if (globalQueue.size() == numPlanNodes) {
          operators.addAll(globalQueue);
        } else {
          throw new RuntimeException(QueryException.SEGMENT_PLAN_EXECUTION_ERROR);
        }
      } catch (InterruptedException e) {
        LOGGER.error("Did not finish executing all segment plans in {} seconds.", TIME_OUT_IN_SECONDS_FOR_PARALLEL_RUN,
            e);
        throw new RuntimeException(QueryException.COMBINE_SEGMENT_PLAN_TIMEOUT_ERROR);
      }
    }
    long end = System.currentTimeMillis();
    LOGGER.debug("CombinePlanNode.run took: {}ms", end - start);

    // TODO: use the same combine operator for both aggregation and selection query.
    if (_brokerRequest.isSetAggregationsInfo() && _brokerRequest.getGroupBy() != null) {
      // Aggregation group-by query.
      return new MCombineGroupByOperator(operators, _executorService, _timeOutMs, _brokerRequest);
    } else {
      // Selection or aggregation only query.
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
