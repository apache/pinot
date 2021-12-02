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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.blocks.CombinedTransformBlock;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;


/**
 * Used for processing a set of TransformOperators, fed by an underlying
 * main predicate transform operator.
 *
 * This class returns a CombinedTransformBlock, with blocks ordered in
 * the order in which their parent filter clauses appear in the query
 */
public class CombinedTransformOperator extends TransformOperator {
  private static final String OPERATOR_NAME = "CombinedTransformOperator";

  protected final List<TransformOperator> _transformOperatorList;
  protected final TransformOperator _mainPredicateTransformOperator;
  protected final BaseFilterOperator _mainPredicateFilterOperator;

  /**
   * Constructor for the class
   */
  public CombinedTransformOperator(List<TransformOperator> transformOperatorList,
      TransformOperator mainPredicateTransformOperator, BaseFilterOperator filterOperator,
      Collection<ExpressionContext> expressions) {
    super(null, transformOperatorList.get(0)._projectionOperator, expressions);

    _mainPredicateTransformOperator = mainPredicateTransformOperator;
    _mainPredicateFilterOperator = filterOperator;
    _transformOperatorList = transformOperatorList;
  }

  @Override
  protected TransformBlock getNextBlock() {
    List<TransformBlock> transformBlockList = new ArrayList<>();
    boolean hasTransformBlock = false;
    boolean isMatchAll = _mainPredicateFilterOperator instanceof MatchAllFilterOperator;
    TransformBlock nonFilteredAggTransformBlock = _mainPredicateTransformOperator.getNextBlock();

    // Get next block from all underlying transform operators
    for (TransformOperator transformOperator : _transformOperatorList) {

      // If it is a match all from main predicate, don't bother broadcasting
      // the block to filter clause predicates
      if (nonFilteredAggTransformBlock != null && !isMatchAll) {
        transformOperator.accept(nonFilteredAggTransformBlock);
      }

      TransformBlock transformBlock = transformOperator.getNextBlock();

      if (transformBlock != null) {
        hasTransformBlock = true;
      }

      transformBlockList.add(transformBlock);
    }


    if (!hasTransformBlock && nonFilteredAggTransformBlock == null) {
      return null;
    }

    return new CombinedTransformBlock(transformBlockList,
        nonFilteredAggTransformBlock);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }
}
