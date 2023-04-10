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
package org.apache.pinot.core.operator;

import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock;
import org.apache.pinot.core.plan.PlanNode;
import org.apache.pinot.segment.spi.FetchContext;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.exception.EarlyTerminationException;


/**
 * A common wrapper around the segment-level operator.
 * NOTE: This is only used if <code>pinot.server.query.executor.enable.prefetch</code> is true
 * This creates a mechanism to acquire and release column buffers before reading data.
 * This Operator is different from others in the following way:
 * It expects the PlanNode of the execution, instead of the Operator.
 * It runs the plan to get the operator, before it begins execution.
 * The reason this is done is the planners access segment buffers,
 * and we need to acquire the segment before any access is made to the buffers.
 */
@SuppressWarnings("unchecked")
public class AcquireReleaseColumnsSegmentOperator extends BaseOperator<BaseResultsBlock> {
  private static final String EXPLAIN_NAME = "ACQUIRE_RELEASE_COLUMNS_SEGMENT";

  private final PlanNode _planNode;
  private final IndexSegment _indexSegment;
  private final FetchContext _fetchContext;

  private Operator<BaseResultsBlock> _childOperator;

  public AcquireReleaseColumnsSegmentOperator(PlanNode planNode, IndexSegment indexSegment, FetchContext fetchContext) {
    _planNode = planNode;
    _indexSegment = indexSegment;
    _fetchContext = fetchContext;
  }

  public void materializeChildOperator() {
    _childOperator = (Operator<BaseResultsBlock>) _planNode.run();
  }

  /**
   * Runs the planNode to get the childOperator, and then proceeds with execution.
   */
  @Override
  protected BaseResultsBlock getNextBlock() {
    materializeChildOperator();
    return _childOperator.nextBlock();
  }

  /**
   * Acquires the indexSegment using the provided fetchContext
   */
  public void acquire() {
    // do not acquire if interrupted (similar to the check inside the nextBlock())
    if (Thread.interrupted()) {
      throw new EarlyTerminationException("Interrupted while acquiring segment");
    }
    _indexSegment.acquire(_fetchContext);
  }

  /**
   * Releases the indexSegment using the provided fetchContext
   */
  public void release() {
    _indexSegment.release(_fetchContext);
  }

  @Override
  public void prepareForExplainPlan(ExplainPlanRows explainPlanRows) {
    acquire();
    materializeChildOperator();
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.singletonList(_childOperator);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _childOperator == null ? new ExecutionStatistics(0, 0, 0, 0) : _childOperator.getExecutionStatistics();
  }
}
