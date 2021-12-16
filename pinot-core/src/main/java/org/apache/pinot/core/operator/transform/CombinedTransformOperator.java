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
package org.apache.pinot.core.operator.transform;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;


/**
 * Used for processing a set of TransformOperators, fed by an underlying
 * main predicate transform operator.
 *
 * This class returns a CombinedTransformBlock, with blocks ordered in
 * the order in which their parent filter clauses appear in the query
 */
public class CombinedTransformOperator extends TransformOperator {
  private static final String OPERATOR_NAME = "CombinedTransformOperator";

  protected final Map<ExpressionContext, TransformOperator> _transformOperatorMap;
  protected final ExpressionContext _mainPredicateExpression;

  /**
   * Constructor for the class
   */
  public CombinedTransformOperator(Map<ExpressionContext, TransformOperator> transformOperatorMap,
      ExpressionContext mainPredicateExpression,
      Collection<ExpressionContext> expressions) {
    super(null, transformOperatorMap.entrySet().iterator().next().getValue()._projectionOperator,
        expressions);

    _mainPredicateExpression = mainPredicateExpression;
    _transformOperatorMap = transformOperatorMap;
  }

  @Override
  protected TransformBlock getNextBlock() {
    Map<ExpressionContext, TransformBlock> expressionContextTransformBlockMap = new HashMap<>();
    boolean hasBlock = false;

    Iterator<Map.Entry<ExpressionContext, TransformOperator>> iterator = _transformOperatorMap.entrySet().iterator();
    // Get next block from all underlying transform operators
    while (iterator.hasNext()) {
      Map.Entry<ExpressionContext, TransformOperator> entry = iterator.next();

      TransformBlock transformBlock = entry.getValue().getNextBlock();

      if (transformBlock != null) {
        hasBlock = true;
      }

      expressionContextTransformBlockMap.put(entry.getKey(), transformBlock);
    }

    if (!hasBlock) {
      return null;
    }

    return new
        CombinedTransformBlock(expressionContextTransformBlockMap,
        _mainPredicateExpression);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
