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
package com.linkedin.pinot.core.operator.transform;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import java.util.List;
import java.util.Map;


/**
 * Class for evaluating transform expressions.
 */
public class TransformExpressionOperator extends BaseOperator<TransformBlock> {
  private static final String OPERATOR_NAME = "TransformExpressionOperator";

  private final MProjectionOperator _projectionOperator;
  TransformExpressionEvaluator _expressionEvaluator;

  /**
   * Constructor for the class
   *
   * @param projectionOperator Projection operator
   * @param expressionTrees Expression tree to evaluate
   */
  public TransformExpressionOperator(MProjectionOperator projectionOperator,
      List<TransformExpressionTree> expressionTrees) {
    Preconditions.checkArgument((projectionOperator != null));

    _projectionOperator = projectionOperator;
    _expressionEvaluator = (expressionTrees != null) ? new DefaultExpressionEvaluator(expressionTrees) : null;
  }

  @Override
  protected TransformBlock getNextBlock() {
    ProjectionBlock projectionBlock = _projectionOperator.nextBlock();
    if (projectionBlock == null) {
      return null;
    }

    Map<String, BlockValSet> expressionResults =
        (_expressionEvaluator != null) ? _expressionEvaluator.evaluate(projectionBlock) : null;

    return new TransformBlock(projectionBlock, expressionResults);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    return _projectionOperator.getExecutionStatistics();
  }

  public int getNumProjectionColumns() {
    return _projectionOperator.getNumProjectionColumns();
  }
}
