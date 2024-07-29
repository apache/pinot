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
import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.combine.AggregationCombineOperator;
import org.apache.pinot.core.operator.combine.BaseCombineOperator;
import org.apache.pinot.core.operator.combine.DistinctCombineOperator;
import org.apache.pinot.core.operator.combine.GroupByCombineOperator;
import org.apache.pinot.core.operator.combine.MinMaxValueBasedSelectionOrderByCombineOperator;
import org.apache.pinot.core.operator.combine.SelectionOnlyCombineOperator;
import org.apache.pinot.core.operator.combine.SelectionOrderByCombineOperator;
import org.apache.pinot.core.operator.streaming.StreamingSelectionOnlyCombineOperator;
import org.apache.pinot.core.query.executor.ResultsBlockStreamer;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextUtils;
import org.apache.pinot.core.util.QueryMultiThreadingUtils;
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
  private final ResultsBlockStreamer _streamer;

  /**
   * Constructor for the class.
   *
   * @param planNodes List of underlying plan nodes
   * @param queryContext Query context
   * @param executorService Executor service
   * @param streamer Optional results block streamer for streaming query
   */
  public CombinePlanNode(List<PlanNode> planNodes, QueryContext queryContext, ExecutorService executorService,
      @Nullable ResultsBlockStreamer streamer) {
    _planNodes = planNodes;
    _queryContext = queryContext;
    _executorService = executorService;
    _streamer = streamer;
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
      int numTasks = QueryMultiThreadingUtils.getNumTasks(numPlanNodes, TARGET_NUM_PLANS_PER_THREAD,
          _queryContext.getMaxExecutionThreads());
      recording.setNumTasks(numTasks);
      QueryMultiThreadingUtils.runTasksWithDeadline(numTasks, index -> {
        List<Operator> ops = new ArrayList<>();
        for (int i = index; i < numPlanNodes; i += numTasks) {
          ops.add(_planNodes.get(i).run());
        }
        return ops;
      }, taskRes -> {
        if (taskRes != null) {
          operators.addAll(taskRes);
        }
      }, e -> {
        // Future object will throw ExecutionException for execution exception, need to check the cause to determine
        // whether it is caused by bad query
        Throwable cause = e.getCause();
        if (cause instanceof BadQueryRequestException) {
          throw (BadQueryRequestException) cause;
        }
        if (e instanceof InterruptedException) {
          throw new QueryCancelledException("Cancelled while running CombinePlanNode", e);
        }
        throw new RuntimeException("Caught exception while running CombinePlanNode.", e);
      }, _executorService, _queryContext.getEndTimeMs());
    }

    if (_streamer != null && QueryContextUtils.isSelectionOnlyQuery(_queryContext) && _queryContext.getLimit() != 0) {
      // Use streaming operator only for non-empty selection-only query
      return new StreamingSelectionOnlyCombineOperator(operators, _queryContext, _executorService);
    } else {
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
          List<OrderByExpressionContext> orderByExpressions = _queryContext.getOrderByExpressions();
          assert orderByExpressions != null;
          if (orderByExpressions.get(0).getExpression().getType() == ExpressionContext.Type.IDENTIFIER) {
            return new MinMaxValueBasedSelectionOrderByCombineOperator(operators, _queryContext, _executorService);
          } else {
            return new SelectionOrderByCombineOperator(operators, _queryContext, _executorService);
          }
        }
      } else {
        assert QueryContextUtils.isDistinctQuery(_queryContext);
        return new DistinctCombineOperator(operators, _queryContext, _executorService);
      }
    }
  }
}
