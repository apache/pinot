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
import com.linkedin.pinot.common.request.transform.TransformFunction;
import com.linkedin.pinot.common.request.transform.result.TransformResult;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import com.linkedin.pinot.core.operator.transform.result.DoubleArrayTransformResult;
import java.util.List;


/**
 * Class for evaluating transform expression.
 */
public class DefaultExpressionEvaluator implements TransformExpressionEvaluator {

  private final TransformExpressionTree _expressionTree;
  private TransformResult _result;

  /**
   * Constructor for the class.
   * @param expressionTree Expression tree to evaluate
   */
  public DefaultExpressionEvaluator(TransformExpressionTree expressionTree) {
    _expressionTree = expressionTree;
  }

  /**
   * {@inheritDoc}
   *
   * @param projectionBlock Projection block to evaluate the expression for.
   */
  @Override
  public void evaluate(ProjectionBlock projectionBlock) {
    _result = evaluateExpression(projectionBlock, _expressionTree);
  }

  /**
   * {@inheritDoc}
   * @return Returns the result of transform expression evaluation.
   */
  @Override
  public TransformResult getResult() {
    return _result;
  }

  /**
   * Helper (recursive) method that walks the expression tree bottom up evaluating
   * transforms at each level.
   *
   * @param projectionBlock Projection block for which to evaluate the expression for
   * @param expressionTree Expression tree to evaluate
   * @return Result of the expression transform
   */
  private TransformResult evaluateExpression(ProjectionBlock projectionBlock, TransformExpressionTree expressionTree) {
    TransformFunction function = expressionTree.getTransform();

    if (function != null) {
      List<TransformExpressionTree> children = expressionTree.getChildren();
      int numChildren = children.size();
      Object[] transformArgs = new Object[numChildren];

      for (int i = 0; i < numChildren; i++) {
        transformArgs[i] = evaluateExpression(projectionBlock, children.get(i)).getResultArray();
      }
      return function.transform(projectionBlock.getNumDocs(), transformArgs);
    } else {
      String column = expressionTree.getColumn();

      // TODO: Support non numeric columns.
      if (column != null) {
        Block dataBlock = projectionBlock.getDataBlock(column);
        ProjectionBlockValSet blockValSet = (ProjectionBlockValSet) dataBlock.getBlockValueSet();
        double[] values = blockValSet.getSingleValues();
        return new DoubleArrayTransformResult(values);
      } else {
        throw new RuntimeException("Literals not supported in transforms yet");
      }
    }
  }
}
