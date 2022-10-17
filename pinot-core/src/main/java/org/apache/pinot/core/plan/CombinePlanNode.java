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
import org.apache.pinot.core.operator.combine.AggregationCombineOperator;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.CombineOperatorUtils;
import org.apache.pinot.core.operator.combine.DistinctCombineOperator;
import org.apache.pinot.core.operator.combine.GroupByCombineOperator;
import org.apache.pinot.core.operator.combine.SelectionOnlyCombineOperator;
import org.apache.pinot.core.operator.combine.SelectionOrderByCombineOperator;
import org.apache.pinot.core.operator.streaming.StreamingSelectionOnlyCombineOperator;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.util.trace.TraceCallable;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.exception.QueryCancelledException;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;


/**
 * The <code>CombinePlanNode</code> class provides the execution plan for combining results from multiple segments.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class CombinePlanNode implements PlanNode {
  // Try to schedule 10 plans for each thread, or evenly distribute plans to all MAX_NUM_THREADS_PER_QUERY threads
  private static final int TARGET_NUM_PLANS_PER_THREAD = 10;

  private final List<PlanNode> _planNodes;
  private final QueryContext _queryContext;
  private final ExecutorService _executorService;
  private final StreamObserver<Server.ServerResponse> _streamObserver;

  /**
   * Constructor for the class.
   *
   * @param planNodes List of underlying plan nodes
   * @param queryContext Query context
   * @param executorService Executor service
   * @param streamObserver Optional stream observer for streaming query
   */
  public CombinePlanNode(List<PlanNode> planNodes, QueryContext queryContext, ExecutorService executorService,
      @Nullable StreamObserver<Server.ServerResponse> streamObserver) {
    _planNodes = planNodes;
    _queryContext = queryContext;
    _executorService = executorService;
    _streamObserver = streamObserver;
  }

  @Override
  public BaseCombineOperator run() {
    try (InvocationScope ignored = Tracing.getTracer().createScope(CombinePlanNode.class)) {
      return getCombineOperator();
    }
  }

  private BaseCombineOperator getCombineOperator() {
    InvocationRecording recording = Tracing.activeRecording();
    int numPlanNodes = _planNodes.size();
    recording.setNumChildren(numPlanNodes);
    List<Operator> operators = new ArrayList<>(numPlanNodes);

    if (numPlanNodes <= TARGET_NUM_PLANS_PER_THREAD) {
      // Small number of plan nodes, run them sequentially
      for (PlanNode planNode : _planNodes) {
        operators.add(planNode.run());
      }
    } else {
      // Large number of plan nodes, run them in parallel
      // NOTE: Even if we get single executor thread, still run it using a separate thread so that the timeout can be
      //       honored

      int maxExecutionThreads = _queryContext.getMaxExecutionThreads();
      if (maxExecutionThreads <= 0) {
        maxExecutionThreads = CombineOperatorUtils.MAX_NUM_THREADS_PER_QUERY;
      }
      int numTasks =
          Math.min((numPlanNodes + TARGET_NUM_PLANS_PER_THREAD - 1) / TARGET_NUM_PLANS_PER_THREAD, maxExecutionThreads);
      recording.setNumTasks(numTasks);

      // Use a Phaser to ensure all the Futures are done (not scheduled, finished or interrupted) before the main thread
      // returns. We need to ensure no execution left before the main thread returning because the main thread holds the
      // reference to the segments, and if the segments are deleted/refreshed, the segments can be released after the
      // main thread returns, which would lead to undefined behavior (even JVM crash) when executing queries against
      // them.
      Phaser phaser = new Phaser(1);

      // Submit all jobs
      Future[] futures = new Future[numTasks];
      for (int i = 0; i < numTasks; i++) {
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
              for (int i = index; i < numPlanNodes; i += numTasks) {
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
          List<Operator> ops = (List<Operator>) future.get(_queryContext.getEndTimeMs() - System.currentTimeMillis(),
              TimeUnit.MILLISECONDS);
          operators.addAll(ops);
        }
      } catch (Exception e) {
        // Future object will throw ExecutionException for execution exception, need to check the cause to determine
        // whether it is caused by bad query
        Throwable cause = e.getCause();
        if (cause instanceof BadQueryRequestException) {
          throw (BadQueryRequestException) cause;
        } else if (e instanceof InterruptedException) {
          throw new QueryCancelledException("Cancelled while running CombinePlanNode", e);
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

    if (_streamObserver != null && QueryContextUtils.isSelectionOnlyQuery(_queryContext)) {
      // Streaming query (only support selection only)
      return new StreamingSelectionOnlyCombineOperator(operators, _queryContext, _executorService, _streamObserver);
    }
    if (QueryContextUtils.isAggregationQuery(_queryContext)) {
      if (_queryContext.getGroupByExpressions() == null) {
        // Aggregation only
        return new AggregationCombineOperator(operators, _queryContext, _executorService);
      } else {
        // Aggregation group-by
        return new GroupByCombineOperator(operators, _queryContext, _executorService);
      }
    } else if (QueryContextUtils.isSelectionQuery(_queryContext)) {
      if (_queryContext.getLimit() == 0 || _queryContext.getOrderByExpressions() == null) {
        // Selection only
        return new SelectionOnlyCombineOperator(operators, _queryContext, _executorService);
      } else {
        // Selection order-by
        return new SelectionOrderByCombineOperator(operators, _queryContext, _executorService);
      }
    } else {
      assert QueryContextUtils.isDistinctQuery(_queryContext);
      return new DistinctCombineOperator(operators, _queryContext, _executorService);
    }
  }
}
