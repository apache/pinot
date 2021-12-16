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
package org.apache.pinot.core.operator.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.blocks.CombinedFilterBlock;
import org.apache.pinot.core.operator.blocks.FilterBlock;
import org.apache.pinot.core.operator.docidsets.AndDocIdSet;


/**
 * A filter operator consisting of one main predicate block and multiple
 * sub blocks. The main predicate block and sub blocks are ANDed before
 * returning.
 */
public class CombinedFilterOperator extends BaseFilterOperator {
  private static final String OPERATOR_NAME = "CombinedFilterOperator";

  protected List<Pair<ExpressionContext, BaseFilterOperator>> _filterOperators;
  protected BaseFilterOperator _mainFilterOperator;
  protected CombinedFilterBlock _resultBlock;

  public CombinedFilterOperator(List<Pair<ExpressionContext, BaseFilterOperator>> filterOperators,
      BaseFilterOperator mainFilterOperator) {
    _filterOperators = filterOperators;
    _mainFilterOperator = mainFilterOperator;
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    List<Operator> operators = new ArrayList<>();

    for (Pair<ExpressionContext, BaseFilterOperator> pair : _filterOperators) {
      operators.add(pair.getRight());
    }

    return operators;
  }

  @Nullable
  @Override
  public String toExplainString() {
    return null;
  }

  @Override
  protected FilterBlock getNextBlock() {
    if (_resultBlock != null) {
      return _resultBlock;
    }

    FilterBlock mainFilterBlock = _mainFilterOperator.nextBlock();

    Map<ExpressionContext, FilterBlock> filterBlockMap = new HashMap<>();
    for (Pair<ExpressionContext, BaseFilterOperator> pair : _filterOperators) {
      FilterBlock subFilterBlock = pair.getValue().nextBlock();
      filterBlockMap.put(pair.getKey(),
          new FilterBlock(new AndDocIdSet(Arrays.asList(subFilterBlock.getBlockDocIdSet(),
              mainFilterBlock.getBlockDocIdSet()))));
    }
    _resultBlock = new CombinedFilterBlock(filterBlockMap, mainFilterBlock);

    return _resultBlock;
  }
}
