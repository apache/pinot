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
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.operator.blocks.ProjectionBlock;
import com.linkedin.pinot.core.operator.docvalsets.ConstantBlockValSet;
import com.linkedin.pinot.core.operator.docvalsets.TransformBlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import com.linkedin.pinot.core.operator.transform.function.TransformFunctionFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Class for evaluating transform expression.
 */
public class DefaultExpressionEvaluator implements TransformExpressionEvaluator {
  private final Set<TransformExpressionTree> _expressionTrees;

  /**
   * Constructor for the class.
   * @param expressionTrees List of expression trees to evaluate
   */
  public DefaultExpressionEvaluator(@Nonnull Set<TransformExpressionTree> expressionTrees) {
    _expressionTrees = expressionTrees;
  }

  /**
   * {@inheritDoc}
   *
   * @param projectionBlock Projection block to evaluate the expression for.
   */
  @Override
  public Map<TransformExpressionTree, BlockValSet> evaluate(ProjectionBlock projectionBlock) {
    // Evaluate each expression once
    Map<TransformExpressionTree, BlockValSet> resultsMap = new HashMap<>(_expressionTrees.size());
    for (TransformExpressionTree expressionTree : _expressionTrees) {
      if (!resultsMap.containsKey(expressionTree)) {
        resultsMap.put(expressionTree, evaluateExpression(projectionBlock, expressionTree));
      }
    }
    return resultsMap;
  }

  /**
   * Helper (recursive) method that walks the expression tree bottom up evaluating
   * transforms at each level.
   *
   * @param projectionBlock Projection block for which to evaluate the expression for
   * @param expressionTree Expression tree to evaluate
   * @return Result of the expression transform
   */
  private BlockValSet evaluateExpression(ProjectionBlock projectionBlock, TransformExpressionTree expressionTree) {
    int numDocs = projectionBlock.getNumDocs();
    TransformExpressionTree.ExpressionType expressionType = expressionTree.getExpressionType();
    switch (expressionType) {
      case FUNCTION:
        List<TransformExpressionTree> children = expressionTree.getChildren();
        int numChildren = children.size();
        BlockValSet[] transformArgs = new BlockValSet[numChildren];
        for (int i = 0; i < numChildren; i++) {
          transformArgs[i] = evaluateExpression(projectionBlock, children.get(i));
        }
        return new TransformBlockValSet(getTransformFunction(expressionTree.getValue()), numDocs, transformArgs);
      case IDENTIFIER:
        return projectionBlock.getBlockValueSet(expressionTree.getValue());
      case LITERAL:
        return new ConstantBlockValSet(expressionTree.getValue(), numDocs);
      default:
        throw new IllegalArgumentException("Illegal expression type in expression evaluator: " + expressionType);
    }
  }

  /**
   * Helper method to get the transform function from the factory
   *
   * @param transformName Name of transform function
   * @return Instance of transform function
   */
  private TransformFunction getTransformFunction(@Nonnull String transformName) {
    return TransformFunctionFactory.get(transformName);
  }
}
