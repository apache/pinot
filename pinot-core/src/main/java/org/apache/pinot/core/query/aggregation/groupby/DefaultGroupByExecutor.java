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
package org.apache.pinot.core.query.aggregation.groupby;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.request.GroupBy;
import org.apache.pinot.common.request.transform.TransformExpressionTree;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.AggregationFunctionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/data-structure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
public class DefaultGroupByExecutor implements GroupByExecutor {
  // Thread local (reusable) array for single-valued group keys
  private static final ThreadLocal<int[]> THREAD_LOCAL_SV_GROUP_KEYS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  // Thread local (reusable) array for multi-valued group keys
  private static final ThreadLocal<int[][]> THREAD_LOCAL_MV_GROUP_KEYS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][]);

  protected final int _numFunctions;
  protected final AggregationFunction[] _functions;
  protected final TransformExpressionTree[] _aggregationExpressions;
  protected final GroupKeyGenerator _groupKeyGenerator;
  protected final GroupByResultHolder[] _resultHolders;
  protected final boolean _hasMVGroupByExpression;
  protected final boolean _hasNoDictionaryGroupByExpression;
  protected final int[] _svGroupKeys;
  protected final int[][] _mvGroupKeys;

  /**
   * Constructor for the class.
   *
   * @param functionContexts Array of aggregation functions
   * @param groupBy Group by from broker request
   * @param maxInitialResultHolderCapacity Maximum initial capacity for the result holder
   * @param numGroupsLimit Limit on number of aggregation groups returned in the result
   * @param transformOperator Transform operator
   */
  public DefaultGroupByExecutor(@Nonnull AggregationFunctionContext[] functionContexts, @Nonnull GroupBy groupBy,
      int maxInitialResultHolderCapacity, int numGroupsLimit, @Nonnull TransformOperator transformOperator) {
    // Initialize aggregation functions and expressions
    _numFunctions = functionContexts.length;
    _functions = new AggregationFunction[_numFunctions];
    _aggregationExpressions = new TransformExpressionTree[_numFunctions];
    for (int i = 0; i < _numFunctions; i++) {
      AggregationFunction function = functionContexts[i].getAggregationFunction();
      _functions[i] = function;
      if (function.getType() != AggregationFunctionType.COUNT) {
        _aggregationExpressions[i] = TransformExpressionTree.compileToExpressionTree(functionContexts[i].getColumnName());
      }
    }

    // Initialize group-by expressions
    List<String> groupByExpressionStrings = groupBy.getExpressions();
    int numGroupByExpressions = groupByExpressionStrings.size();
    boolean hasMVGroupByExpression = false;
    boolean hasNoDictionaryGroupByExpression = false;
    TransformExpressionTree[] groupByExpressions = new TransformExpressionTree[numGroupByExpressions];
    for (int i = 0; i < numGroupByExpressions; i++) {
      groupByExpressions[i] = TransformExpressionTree.compileToExpressionTree(groupByExpressionStrings.get(i));
      TransformResultMetadata transformResultMetadata = transformOperator.getResultMetadata(groupByExpressions[i]);
      hasMVGroupByExpression |= !transformResultMetadata.isSingleValue();
      hasNoDictionaryGroupByExpression |= !transformResultMetadata.hasDictionary();
    }
    _hasMVGroupByExpression = hasMVGroupByExpression;
    _hasNoDictionaryGroupByExpression = hasNoDictionaryGroupByExpression;

    // Initialize group key generator
    if (_hasNoDictionaryGroupByExpression) {
      if (numGroupByExpressions == 1) {
        _groupKeyGenerator =
            new NoDictionarySingleColumnGroupKeyGenerator(transformOperator, groupByExpressions[0], numGroupsLimit);
      } else {
        _groupKeyGenerator =
            new NoDictionaryMultiColumnGroupKeyGenerator(transformOperator, groupByExpressions, numGroupsLimit);
      }
    } else {
      _groupKeyGenerator = new DictionaryBasedGroupKeyGenerator(transformOperator, groupByExpressions, numGroupsLimit,
          maxInitialResultHolderCapacity);
    }

    // Initialize result holders
    int maxNumResults = _groupKeyGenerator.getGlobalGroupKeyUpperBound();
    int initialCapacity = Math.min(maxNumResults, maxInitialResultHolderCapacity);
    _resultHolders = new GroupByResultHolder[_numFunctions];
    for (int i = 0; i < _numFunctions; i++) {
      _resultHolders[i] = _functions[i].createGroupByResultHolder(initialCapacity, maxNumResults);
    }

    // Initialize map from document Id to group key
    if (_hasMVGroupByExpression) {
      _svGroupKeys = null;
      _mvGroupKeys = THREAD_LOCAL_MV_GROUP_KEYS.get();
    } else {
      _svGroupKeys = THREAD_LOCAL_SV_GROUP_KEYS.get();
      _mvGroupKeys = null;
    }
  }

  @Override
  public void process(@Nonnull TransformBlock transformBlock) {
    // Generate group keys
    // NOTE: groupKeyGenerator will limit the number of groups. Once reaching limit, no new group will be generated
    if (_hasMVGroupByExpression) {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _mvGroupKeys);
    } else {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _svGroupKeys);
    }

    int length = transformBlock.getNumDocs();
    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();
    for (int i = 0; i < _numFunctions; i++) {
      GroupByResultHolder resultHolder = _resultHolders[i];
      resultHolder.ensureCapacity(capacityNeeded);
      aggregate(transformBlock, length, i);
    }
  }

  protected void aggregate(@Nonnull TransformBlock transformBlock, int length, int functionIndex) {
    AggregationFunction function = _functions[functionIndex];
    GroupByResultHolder resultHolder = _resultHolders[functionIndex];

    if (function.getType() == AggregationFunctionType.COUNT) {
      if (_hasMVGroupByExpression) {
        function.aggregateGroupByMV(length, _mvGroupKeys, resultHolder, Collections.emptyMap());
      } else {
        function.aggregateGroupBySV(length, _svGroupKeys, resultHolder, Collections.emptyMap());
      }
    } else {
      TransformExpressionTree aggregationExpression = _aggregationExpressions[functionIndex];
      Map<String, BlockValSet> blockValSetMap = Collections
          .singletonMap(aggregationExpression.toString(), transformBlock.getBlockValueSet(aggregationExpression));
      if (_hasMVGroupByExpression) {
        function.aggregateGroupByMV(length, _mvGroupKeys, resultHolder, blockValSetMap);
      } else {
        function.aggregateGroupBySV(length, _svGroupKeys, resultHolder, blockValSetMap);
      }
    }
  }

  @Override
  public AggregationGroupByResult getResult() {
    return new AggregationGroupByResult(_groupKeyGenerator, _functions, _resultHolders);
  }
}
