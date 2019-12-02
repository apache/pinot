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
package org.apache.pinot.core.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.CombineGroupByOperator;
import org.apache.pinot.core.operator.CombineGroupByOrderByOperator;
import org.apache.pinot.core.operator.CombineOperator;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The <code>CombinePlanNode</code> class provides the execution plan for combining results from multiple segments.
 */
public class CombinePlanNode implements PlanNode {
  private static final Logger LOGGER = LoggerFactory.getLogger(CombinePlanNode.class);

  // Use at most 10 or half of the processors threads for each query.
  // If there are less than 2 processors, use 1 thread.
  // Runtime.getRuntime().availableProcessors() may return value < 2 in container based environment, e.g. Kubernetes.
  private static final int MAX_NUM_THREADS_PER_QUERY =
      Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));
  // Try to schedule 10 plans for each thread, or evenly distribute plans to all MAX_NUM_THREADS_PER_QUERY threads
  private static final int TARGET_NUM_PLANS_PER_THREAD = 10;

  private static final int TIME_OUT_IN_MILLISECONDS_FOR_PARALLEL_RUN = 10_000;

  private final List<PlanNode> _planNodes;
  private final BrokerRequest _brokerRequest;
  private final ExecutorService _executorService;
  private final long _timeOutMs;
  private final int _numGroupsLimit;

  /**
   * Constructor for the class.
   *
   * @param planNodes List of underlying plan nodes
   * @param brokerRequest Broker request
   * @param executorService Executor service
   * @param timeOutMs Time out in milliseconds for query execution (not for planning phase)
   * @param numGroupsLimit Limit of number of groups stored in each segment
   */
  public CombinePlanNode(List<PlanNode> planNodes, BrokerRequest brokerRequest, ExecutorService executorService,
      long timeOutMs, int numGroupsLimit) {
    _planNodes = planNodes;
    _brokerRequest = brokerRequest;
    _executorService = executorService;
    _timeOutMs = timeOutMs;
    _numGroupsLimit = numGroupsLimit;
  }

  @Override
  public Operator run() {
    int numPlanNodes = _planNodes.size();
    List<Operator> operators = new ArrayList<>(numPlanNodes);

    if (numPlanNodes <= TARGET_NUM_PLANS_PER_THREAD) {
      // Small number of plan nodes, run them sequentially
      for (PlanNode planNode : _planNodes) {
        operators.add(planNode.run());
      }
    } else {
      // Large number of plan nodes, run them in parallel

      // Calculate the time out timestamp
      long endTimeMs = System.currentTimeMillis() + TIME_OUT_IN_MILLISECONDS_FOR_PARALLEL_RUN;

      int numThreads = Math.min((numPlanNodes + TARGET_NUM_PLANS_PER_THREAD - 1) / TARGET_NUM_PLANS_PER_THREAD,
          MAX_NUM_THREADS_PER_QUERY);

      // Use a Phaser to ensure all the Futures are done (not scheduled, finished or interrupted) before the main thread
      // returns. We need to ensure no execution left before the main thread returning because the main thread holds the
      // reference to the segments, and if the segments are deleted/refreshed, the segments can be released after the
      // main thread returns, which would lead to undefined behavior (even JVM crash) when executing queries against
      // them.
      Phaser phaser = new Phaser(1);

      // Submit all jobs
      Future[] futures = new Future[numThreads];
      for (int i = 0; i < numThreads; i++) {
        int index = i;
        futures[i] = _executorService.submit(new TraceCallable<List<Operator>>() {
          @Override
          public List<Operator> callJob() {
            try {
              // Register the thread to the phaser.
              // If the phaser is terminated (returning negative value) when trying to register the thread, that means
              // the query execution has timed out, and the main thread has deregistered itself and returned the result.
              // Directly return as no execution result will be taken.
              if (phaser.register() < 0) {
                return Collections.emptyList();
              }

              List<Operator> operators = new ArrayList<>();
              for (int i = index; i < numPlanNodes; i += numThreads) {
                operators.add(_planNodes.get(i).run());
              }
              return operators;
            } finally {
              phaser.arriveAndDeregister();
            }
          }
        });
      }

      // Get all results
      try {
        for (Future future : futures) {
          List<Operator> ops =
              (List<Operator>) future.get(endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          operators.addAll(ops);
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
        // Deregister the main thread and wait for all threads done
        phaser.awaitAdvance(phaser.arriveAndDeregister());
      }
    }

    // TODO: use the same combine operator for both aggregation and selection query.
    if (_brokerRequest.isSetAggregationsInfo() && _brokerRequest.getGroupBy() != null) {
      // Aggregation group-by query
      QueryOptions queryOptions = new QueryOptions(_brokerRequest.getQueryOptions());
      // new Combine operator only when GROUP_BY_MODE explicitly set to SQL
      if (queryOptions.isGroupByModeSQL()) {
        return new CombineGroupByOrderByOperator(operators, _brokerRequest, _executorService, _timeOutMs);
      }
      return new CombineGroupByOperator(operators, _brokerRequest, _executorService, _timeOutMs, _numGroupsLimit);
    } else {
      // Selection or aggregation only query
      return new CombineOperator(operators, _executorService, _timeOutMs, _brokerRequest);
    }
  }

  @Override
  public void showTree(String prefix) {
    LOGGER.debug(prefix + "Instance Level Inter-Segments Combine Plan Node:");
    LOGGER.debug(prefix + "Operator: CombineOperator/CombineGroupByOperator");
    LOGGER.debug(prefix + "Argument 0: BrokerRequest - " + _brokerRequest);
    int i = 1;
    for (PlanNode planNode : _planNodes) {
      LOGGER.debug(prefix + "Argument " + (i++) + ":");
      planNode.showTree(prefix + "    ");
    }
  }
}
