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
package org.apache.pinot.query.runtime.operator;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.common.ExplainPlanRows;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.query.planner.stage.SetOpNode;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.segment.spi.IndexSegment;


public abstract class SetOperator extends MultiStageOperator {
  private final List<MultiStageOperator> _upstreamOperators;

  public SetOperator(OpChainExecutionContext opChainExecutionContext, List<MultiStageOperator> upstreamOperators,
      DataSchema dataSchema, SetOpNode setOpNode) {
    super(opChainExecutionContext);
    _upstreamOperators = upstreamOperators;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return _upstreamOperators;
  }

  @Override
  public void prepareForExplainPlan(ExplainPlanRows explainPlanRows) {
    super.prepareForExplainPlan(explainPlanRows);
  }

  @Override
  public void explainPlan(ExplainPlanRows explainPlanRows, int[] globalId, int parentId) {
    super.explainPlan(explainPlanRows, globalId, parentId);
  }

  @Override
  public IndexSegment getIndexSegment() {
    return super.getIndexSegment();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return super.getExecutionStatistics();
  }
}
