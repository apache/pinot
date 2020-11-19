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

import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.operator.combine.AggregationOnlyCombineOperator;
import org.apache.pinot.core.operator.combine.GroupByCombineOperator;
import org.apache.pinot.core.operator.combine.GroupByOrderByCombineOperator;
import org.apache.pinot.core.operator.combine.SelectionOnlyCombineOperator;
import org.apache.pinot.core.operator.combine.SelectionOrderByCombineOperator;
import org.apache.pinot.core.operator.streaming.StreamingSelectionOnlyCombineOperator;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.core.util.trace.TraceCallable;


/**
 * The <code>CombinePlanNode</code> class provides the execution plan for combining results from multiple segments.
 */
public class CombinePlanNode implements PlanNode {
  // Use at most 10 or half of the processors threads for each query.
  // If there are less than 2 processors, use 1 thread.
  // Runtime.getRuntime().availableProcessors() may return value < 2 in container based environment, e.g. Kubernetes.
  private static final int MAX_NUM_THREADS_PER_QUERY =
      Math.max(1, Math.min(10, Runtime.getRuntime().availableProcessors() / 2));
  // Try to schedule 10 plans for each thread, or evenly distribute plans to all MAX_NUM_THREADS_PER_QUERY threads
  private static final int TARGET_NUM_PLANS_PER_THREAD = 10;

  private final List<PlanNode> _planNodes;
  private final QueryContext _queryContext;
  private final ExecutorService _executorService;
  private final long _endTimeMs;
  private final int _numGroupsLimit;
  private final StreamObserver<Server.ServerResponse> _streamObserver;
  // used for SQL GROUP BY during server combine
  private final int _groupByTrimThreshold;

  /**
   * Constructor for the class.
   *
   * @param planNodes List of underlying plan nodes
   * @param queryContext Query context
   * @param executorService Executor service
   * @param endTimeMs End time in milliseconds for the query
   * @param numGroupsLimit Limit of number of groups stored in each segment
   * @param streamObserver Optional stream observer for streaming query
   * @param groupByTrimThreshold trim threshold to use for server combine for SQL GROUP BY
   */
  public CombinePlanNode(List<PlanNode> planNodes, QueryContext queryContext, ExecutorService executorService,
      long endTimeMs, int numGroupsLimit, @Nullable StreamObserver<Server.ServerResponse> streamObserver,
      int groupByTrimThreshold) {
    _planNodes = planNodes;
    _queryContext = queryContext;
    _executorService = executorService;
    _endTimeMs = endTimeMs;
    _numGroupsLimit = numGroupsLimit;
    _streamObserver = streamObserver;
    _groupByTrimThreshold = groupByTrimThreshold;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public Operator<IntermediateResultsBlock> run() {
    int numPlanNodes = _planNodes.size();
    List<Operator> operators = new ArrayList<>(numPlanNodes);

    if (numPlanNodes <= TARGET_NUM_PLANS_PER_THREAD) {
      // Small number of plan nodes, run them sequentially
      for (PlanNode planNode : _planNodes) {
        operators.add(planNode.run());
      }
    } else {
      // Large number of plan nodes, run them in parallel

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
              (List<Operator>) future.get(_endTimeMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
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

    if (_streamObserver != null) {
      // Streaming query (only support selection only)
      return new StreamingSelectionOnlyCombineOperator(operators, _queryContext, _executorService, _endTimeMs,
          _streamObserver);
    }
    if (QueryContextUtils.isAggregationQuery(_queryContext)) {
      if (_queryContext.getGroupByExpressions() == null) {
        // Aggregation only
        return new AggregationOnlyCombineOperator(operators, _queryContext, _executorService, _endTimeMs);
      } else {
        // Aggregation group-by
        QueryOptions queryOptions = new QueryOptions(_queryContext.getQueryOptions());
        if (queryOptions.isGroupByModeSQL()) {
          return new GroupByOrderByCombineOperator(operators, _queryContext, _executorService, _endTimeMs,
              _groupByTrimThreshold);
        }
        return new GroupByCombineOperator(operators, _queryContext, _executorService, _endTimeMs, _numGroupsLimit);
      }
    } else {
      if (_queryContext.getLimit() == 0 || _queryContext.getOrderByExpressions() == null) {
        // Selection only
        return new SelectionOnlyCombineOperator(operators, _queryContext, _executorService, _endTimeMs);
      } else {
        // Selection order-by
        return new SelectionOrderByCombineOperator(operators, _queryContext, _executorService, _endTimeMs);
      }
    }
  }
}
