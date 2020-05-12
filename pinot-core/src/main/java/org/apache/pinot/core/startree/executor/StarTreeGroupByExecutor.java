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
package org.apache.pinot.core.startree.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.DefaultGroupByExecutor;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.startree.StarTreeUtils;
import org.apache.pinot.core.startree.v2.AggregationFunctionColumnPair;


/**
 * The <code>StarTreeGroupByExecutor</code> class is the group-by executor for star-tree index.
 * <ul>
 *   <li>The column in function context is function-column pair</li>
 *   <li>No UDF in aggregation</li>
 *   <li>For <code>COUNT</code> aggregation function, we need to aggregate on the pre-aggregated column</li>
 * </ul>
 */
public class StarTreeGroupByExecutor extends DefaultGroupByExecutor {

  // StarTree converts column names from min(col) to min__col, this is to store the original mapping.
  private final String[][] _functionArgs;

  public StarTreeGroupByExecutor(@Nonnull AggregationFunctionContext[] functionContexts, @Nonnull GroupBy groupBy,
      int maxInitialResultHolderCapacity, int numGroupsLimit, @Nonnull TransformOperator transformOperator) {
    super(StarTreeUtils.createStarTreeFunctionContexts(functionContexts), groupBy, maxInitialResultHolderCapacity,
        numGroupsLimit, transformOperator);

    _functionArgs = new String[functionContexts.length][];
    for (int i = 0; i < functionContexts.length; i++) {
      List<String> expressions = functionContexts[i].getExpressions();
      _functionArgs[i] = new String[expressions.size()];

      for (int j = 0; j < expressions.size(); j++) {
        _functionArgs[i][j] = expressions.get(j);
      }
    }
  }

  @Override
  protected void aggregate(@Nonnull TransformBlock transformBlock, int length, int functionIndex) {
    AggregationFunction function = _functions[functionIndex];
    GroupByResultHolder resultHolder = _resultHolders[functionIndex];

    BlockValSet blockValueSet;
    Map<String, BlockValSet> blockValSetMap;

    AggregationFunctionType functionType = function.getType();
    if (functionType == AggregationFunctionType.COUNT) {
      blockValueSet = transformBlock.getBlockValueSet(AggregationFunctionColumnPair.COUNT_STAR_COLUMN_NAME);
      function.getInputExpressions();
      blockValSetMap = Collections.singletonMap(_functionArgs[functionIndex][0], blockValueSet);
    } else {

      blockValSetMap = new HashMap<>();
      for (int i = 0; i < _functionArgs[functionIndex].length; i++) {
        TransformExpressionTree aggregationExpression = _aggregationExpressions[functionIndex][i];
        blockValueSet = transformBlock.getBlockValueSet(
            AggregationFunctionColumnPair.toColumnName(functionType, aggregationExpression.getValue()));
        blockValSetMap.put(_functionArgs[functionIndex][i], blockValueSet);
      }
    }

    if (_hasMVGroupByExpression) {
      function.aggregateGroupByMV(length, _mvGroupKeys, resultHolder, blockValSetMap);
    } else {
      function.aggregateGroupBySV(length, _svGroupKeys, resultHolder, blockValSetMap);
    }
  }
}