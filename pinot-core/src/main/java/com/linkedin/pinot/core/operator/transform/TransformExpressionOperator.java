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

import com.linkedin.pinot.common.request.transform.TransformExpressionTree;
import com.linkedin.pinot.core.operator.BaseOperator;
import com.linkedin.pinot.core.operator.ExecutionStatistics;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.blocks.TransformBlock;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Class for evaluating transform expressions.
 */
public class TransformExpressionOperator extends BaseOperator<TransformBlock> {
  private static final String OPERATOR_NAME = "TransformExpressionOperator";

  private final MProjectionOperator _projectionOperator;
  private final TransformExpressionEvaluator _expressionEvaluator;

  /**
   * Constructor for the class
   *
   * @param projectionOperator Projection operator
   * @param expressionTrees Set of expression trees to evaluate
   */
  public TransformExpressionOperator(@Nonnull MProjectionOperator projectionOperator,
      @Nonnull Set<TransformExpressionTree> expressionTrees) {
    _projectionOperator = projectionOperator;
    _expressionEvaluator = new DefaultExpressionEvaluator(expressionTrees);
  }

  @Override
  protected TransformBlock getNextBlock() {
    ProjectionBlock projectionBlock = _projectionOperator.nextBlock();
    if (projectionBlock == null) {
      return null;
    } else {
      return new TransformBlock(projectionBlock, _expressionEvaluator.evaluate(projectionBlock));
    }
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
