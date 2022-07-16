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

import java.util.Collection;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.request.context.QueryContext;


/**
 * This class implements group by aggregation.
 * It is optimized for performance, and uses the best possible algorithm/data-structure
 * for a given query based on the following parameters:
 * - Maximum number of group keys possible.
 * - Single/Multi valued columns.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultGroupByExecutor implements GroupByExecutor {
  // Thread local (reusable) array for single-valued group keys
  private static final ThreadLocal<int[]> THREAD_LOCAL_SV_GROUP_KEYS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL]);

  // Thread local (reusable) array for multi-valued group keys
  private static final ThreadLocal<int[][]> THREAD_LOCAL_MV_GROUP_KEYS =
      ThreadLocal.withInitial(() -> new int[DocIdSetPlanNode.MAX_DOC_PER_CALL][]);

  protected final AggregationFunction[] _aggregationFunctions;
  protected final boolean _nullHandlingEnabled;
  protected final GroupKeyGenerator _groupKeyGenerator;
  protected final GroupByResultHolder[] _groupByResultHolders;
  protected final boolean _hasMVGroupByExpression;
  protected final int[] _svGroupKeys;
  protected final int[][] _mvGroupKeys;

  /**
   * Constructor for the class.
   *
   * @param queryContext Query context
   * @param groupByExpressions Array of group-by expressions
   * @param transformOperator Transform operator
   */
  public DefaultGroupByExecutor(QueryContext queryContext, ExpressionContext[] groupByExpressions,
      TransformOperator transformOperator) {
    _aggregationFunctions = queryContext.getAggregationFunctions();
    assert _aggregationFunctions != null;
    _nullHandlingEnabled = queryContext.isNullHandlingEnabled();

    boolean hasMVGroupByExpression = false;
    boolean hasNoDictionaryGroupByExpression = false;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      TransformResultMetadata transformResultMetadata = transformOperator.getResultMetadata(groupByExpression);
      hasMVGroupByExpression |= !transformResultMetadata.isSingleValue();
      hasNoDictionaryGroupByExpression |= !transformResultMetadata.hasDictionary();
    }
    _hasMVGroupByExpression = hasMVGroupByExpression;

    // Initialize group key generator
    int numGroupsLimit = queryContext.getNumGroupsLimit();
    int maxInitialResultHolderCapacity = queryContext.getMaxInitialResultHolderCapacity();
    if (hasNoDictionaryGroupByExpression || _nullHandlingEnabled) {
      if (groupByExpressions.length == 1) {
        // TODO(nhejazi): support MV and dictionary based when null handling is enabled.
        _groupKeyGenerator =
            new NoDictionarySingleColumnGroupKeyGenerator(transformOperator, groupByExpressions[0], numGroupsLimit,
                _nullHandlingEnabled);
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
    int numAggregationFunctions = _aggregationFunctions.length;
    _groupByResultHolders = new GroupByResultHolder[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _groupByResultHolders[i] = _aggregationFunctions[i].createGroupByResultHolder(initialCapacity, maxNumResults);
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
  public void process(TransformBlock transformBlock) {
    // Generate group keys
    // NOTE: groupKeyGenerator will limit the number of groups. Once reaching limit, no new group will be generated
    if (_hasMVGroupByExpression) {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _mvGroupKeys);
    } else {
      _groupKeyGenerator.generateKeysForBlock(transformBlock, _svGroupKeys);
    }

    int capacityNeeded = _groupKeyGenerator.getCurrentGroupKeyUpperBound();
    int length = transformBlock.getNumDocs();
    int numAggregationFunctions = _aggregationFunctions.length;
    for (int i = 0; i < numAggregationFunctions; i++) {
      GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
      groupByResultHolder.ensureCapacity(capacityNeeded);
      aggregate(transformBlock, length, i);
    }
  }

  protected void aggregate(TransformBlock transformBlock, int length, int functionIndex) {
    AggregationFunction aggregationFunction = _aggregationFunctions[functionIndex];
    Map<ExpressionContext, BlockValSet> blockValSetMap =
        AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, transformBlock);

    GroupByResultHolder groupByResultHolder = _groupByResultHolders[functionIndex];
    if (_hasMVGroupByExpression) {
      aggregationFunction.aggregateGroupByMV(length, _mvGroupKeys, groupByResultHolder, blockValSetMap);
    } else {
      aggregationFunction.aggregateGroupBySV(length, _svGroupKeys, groupByResultHolder, blockValSetMap);
    }
  }

  @Override
  public AggregationGroupByResult getResult() {
    return new AggregationGroupByResult(_groupKeyGenerator, _aggregationFunctions, _groupByResultHolders);
  }

  @Override
  public int getNumGroups() {
    return _groupKeyGenerator.getNumKeys();
  }

  @Override
  public Collection<IntermediateRecord> trimGroupByResult(int trimSize, TableResizer tableResizer) {
    return tableResizer.trimInSegmentResults(_groupKeyGenerator, _groupByResultHolders, trimSize);
  }
}
