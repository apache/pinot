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
import javax.annotation.Nonnull;


/**
 * Class for evaluating transform expression.
 */
public class DefaultExpressionEvaluator implements TransformExpressionEvaluator {

  private final List<TransformExpressionTree> _expressionTrees;

  /**
   * Constructor for the class.
   * @param expressionTrees List of expression trees to evaluate
   */
  public DefaultExpressionEvaluator(@Nonnull List<TransformExpressionTree> expressionTrees) {
    _expressionTrees = expressionTrees;
  }

  /**
   * {@inheritDoc}
   *
   * @param projectionBlock Projection block to evaluate the expression for.
   */
  @Override
  public Map<String, BlockValSet> evaluate(ProjectionBlock projectionBlock) {
    Map<String, BlockValSet> resultsMap = new HashMap<>(_expressionTrees.size());

    for (TransformExpressionTree expressionTree : _expressionTrees) {
      // Enough to evaluate an expression once.
      if (!resultsMap.containsKey(expressionTree.toString())) {
        resultsMap.put(expressionTree.toString(), evaluateExpression(projectionBlock, expressionTree));
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
  private BlockValSet evaluateExpression(ProjectionBlock projectionBlock,
      TransformExpressionTree expressionTree) {
    TransformFunction function = getTransformFunction(expressionTree.getTransformName());

    int numDocs = projectionBlock.getNumDocs();
    String expressionString = expressionTree.toString();
    TransformExpressionTree.ExpressionType expressionType = expressionTree.getExpressionType();

    switch (expressionType) {
      case FUNCTION:
        List<TransformExpressionTree> children = expressionTree.getChildren();
        int numChildren = children.size();
        BlockValSet[] transformArgs = new BlockValSet[numChildren];

        for (int i = 0; i < numChildren; i++) {
          transformArgs[i] = evaluateExpression(projectionBlock, children.get(i));
        }
        return new TransformBlockValSet(function, numDocs, transformArgs);

      case IDENTIFIER:
        return projectionBlock.getBlockValueSet(expressionString);

      case LITERAL:
        return new ConstantBlockValSet(expressionString, numDocs);

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
  private TransformFunction getTransformFunction(String transformName) {
    return (transformName != null) ? TransformFunctionFactory.get(transformName) : null;
  }
}
