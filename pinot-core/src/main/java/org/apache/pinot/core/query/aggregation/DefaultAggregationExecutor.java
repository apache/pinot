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
package org.apache.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


public class DefaultAggregationExecutor implements AggregationExecutor {
  protected final int _numFunctions;
  protected final AggregationFunction[] _functions;
  protected final AggregationResultHolder[] _resultHolders;
  protected final TransformExpressionTree[] _expressions;

  public DefaultAggregationExecutor(AggregationFunctionContext[] functionContexts) {
    _numFunctions = functionContexts.length;
    _functions = new AggregationFunction[_numFunctions];
    _resultHolders = new AggregationResultHolder[_numFunctions];
    if (AggregationFunctionUtils.isDistinct(functionContexts)) {
      // handle distinct (col1, col2..) function
      // unlike other aggregate functions, distinct can work on multiple columns
      // so we need to build expression tree for each column
      _functions[0] = functionContexts[0].getAggregationFunction();
      _resultHolders[0] = _functions[0].createAggregationResultHolder();

      List<String> expressions = functionContexts[0].getExpressions();
      _expressions = new TransformExpressionTree[expressions.size()];

      for (int i = 0; i < _expressions.length; i++) {
        _expressions[i] = TransformExpressionTree.compileToExpressionTree(expressions.get(i));

      }
    } else {
      _expressions = new TransformExpressionTree[_numFunctions];
      for (int i = 0; i < _numFunctions; i++) {
        AggregationFunction function = functionContexts[i].getAggregationFunction();
        _functions[i] = function;
        _resultHolders[i] = _functions[i].createAggregationResultHolder();
        if (function.getType() != AggregationFunctionType.COUNT) {
          // count(*) does not have a column so handle rest of the aggregate
          // functions -- sum, min, max etc
          _expressions[i] = TransformExpressionTree.compileToExpressionTree(functionContexts[i].getColumnName());
        }
      }
    }
  }

  @Override
  public void aggregate(TransformBlock transformBlock) {
    int length = transformBlock.getNumDocs();
    for (int i = 0; i < _numFunctions; i++) {
      AggregationFunction function = _functions[i];
      AggregationResultHolder resultHolder = _resultHolders[i];
      if (function.getType() == AggregationFunctionType.COUNT) {
        // handle count(*) function
        function.aggregate(length, resultHolder, Collections.emptyMap());
      } else if (function.getType() == AggregationFunctionType.DISTINCT) {
        // handle distinct (col1, col2..) function
        // unlike other aggregate functions, distinct can work on multiple columns
        // so we get all the projected columns (ProjectionBlockValSet) from TransformBlock
        // for each column and then pass them over to distinct function since the uniqueness
        // will be determined across tuples and not on a per column basis
        Map<String, BlockValSet> blockValSetMap = new HashMap<>();
        for (int j = 0; j < _expressions.length; j++) {
          blockValSetMap.put(_expressions[j].toString(), transformBlock.getBlockValueSet(_expressions[j]));
        }
        function.aggregate(length, resultHolder, blockValSetMap);
      } else {
        // handle rest of the aggregate functions -- sum, min, max etc
        function.aggregate(length, resultHolder,
            Collections.singletonMap(_expressions[i].toString(), transformBlock.getBlockValueSet(_expressions[i])));
      }
    }
  }

  @Override
  public List<Object> getResult() {
    List<Object> aggregationResults = new ArrayList<>(_numFunctions);
    for (int i = 0; i < _numFunctions; i++) {
      aggregationResults.add(_functions[i].extractAggregationResult(_resultHolders[i]));
    }
    return aggregationResults;
  }
}
