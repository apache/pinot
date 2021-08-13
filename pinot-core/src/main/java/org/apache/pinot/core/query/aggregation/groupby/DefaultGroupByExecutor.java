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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.DictIdRecord;
import org.apache.pinot.core.data.table.IntermediateRecord;
import org.apache.pinot.core.data.table.TableResizer;
import org.apache.pinot.core.operator.blocks.TransformBlock;
import org.apache.pinot.core.operator.transform.TransformOperator;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.util.QueryOptions;

import static org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator.Type.DictArray;


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
  protected static final int ON_THE_FLY_TRIM_THRESHOLD = 100000;
  protected static final int ON_THE_FLY_TRIM_SIZE = 5000;
  protected final AggregationFunction[] _aggregationFunctions;
  protected final GroupKeyGenerator _groupKeyGenerator;
  protected final GroupByResultHolder[] _groupByResultHolders;
  protected final boolean _hasMVGroupByExpression;
  protected final int[] _svGroupKeys;
  protected final int[][] _mvGroupKeys;
  protected boolean _onTheFlyTrimFlag;
  protected final boolean _hasNoDictionaryGroupByExpression;
  protected final int _numGroupByExpressions;
  protected final TableResizer _tableResizer;
  protected final int _onTheFlyTrimSize;
  protected final int _onTheFlyTrimThreshold;

  /**
   * Constructor for the class.
   *
   * @param aggregationFunctions Array of aggregation functions
   * @param groupByExpressions Array of group-by expressions
   * @param maxInitialResultHolderCapacity Maximum initial capacity for the result holder
   * @param numGroupsLimit Limit on number of aggregation groups returned in the result
   * @param transformOperator Transform operator
   * @param tableResizer table resizer
   * @param queryOptions query options for potential trim parameter
   */
  public DefaultGroupByExecutor(AggregationFunction[] aggregationFunctions, ExpressionContext[] groupByExpressions,
      int maxInitialResultHolderCapacity, int numGroupsLimit, TransformOperator transformOperator,
      TableResizer tableResizer, Map<String, String> queryOptions) {
    _aggregationFunctions = aggregationFunctions;

    boolean hasMVGroupByExpression = false;
    boolean hasNoDictionaryGroupByExpression = false;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      TransformResultMetadata transformResultMetadata = transformOperator.getResultMetadata(groupByExpression);
      hasMVGroupByExpression |= !transformResultMetadata.isSingleValue();
      hasNoDictionaryGroupByExpression |= !transformResultMetadata.hasDictionary();
    }
    _hasMVGroupByExpression = hasMVGroupByExpression;
    _numGroupByExpressions = groupByExpressions.length;
    _hasNoDictionaryGroupByExpression = hasNoDictionaryGroupByExpression;


    if (queryOptions != null) {
      Integer trimSize = QueryOptions.getMinSegmentGroupTrimSize(queryOptions);
      Integer threshold = QueryOptions.getMinSegmentGroupTrimThreshold(queryOptions);
      _onTheFlyTrimSize = trimSize != null ? trimSize : ON_THE_FLY_TRIM_SIZE;
      _onTheFlyTrimThreshold = threshold != null ? threshold : ON_THE_FLY_TRIM_THRESHOLD;
      _onTheFlyTrimFlag = _onTheFlyTrimThreshold > 0 && _onTheFlyTrimSize > 0;
    } else {
      _onTheFlyTrimSize = -1;
      _onTheFlyTrimThreshold = -1;
      _onTheFlyTrimFlag = false;
    }

    // Initialize group key generator
    if (hasNoDictionaryGroupByExpression) {
      if (groupByExpressions.length == 1) {
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

    _tableResizer = tableResizer;

    // Initialize result holders
    int maxNumResults = _groupKeyGenerator.getGlobalGroupKeyUpperBound();
    int initialCapacity = Math.min(maxNumResults, maxInitialResultHolderCapacity);
    int numAggregationFunctions = aggregationFunctions.length;
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

  /**
   * Constructor for the class. Used for test only
   *
   * @param aggregationFunctions Array of aggregation functions
   * @param groupByExpressions Array of group-by expressions
   * @param maxInitialResultHolderCapacity Maximum initial capacity for the result holder
   * @param numGroupsLimit Limit on number of aggregation groups returned in the result
   * @param transformOperator Transform operator
   * @param tableResizer table resizer
   * @param queryOptions query options for potential trim parameter
   * @param keyGenType specific key generator type used
   */
  @VisibleForTesting
  public DefaultGroupByExecutor(AggregationFunction[] aggregationFunctions, ExpressionContext[] groupByExpressions,
      int maxInitialResultHolderCapacity, int numGroupsLimit, TransformOperator transformOperator,
      TableResizer tableResizer, Map<String, String> queryOptions, GroupKeyGenerator.Type keyGenType) {
    _aggregationFunctions = aggregationFunctions;

    boolean hasMVGroupByExpression = false;
    boolean hasNoDictionaryGroupByExpression = false;
    for (ExpressionContext groupByExpression : groupByExpressions) {
      TransformResultMetadata transformResultMetadata = transformOperator.getResultMetadata(groupByExpression);
      hasMVGroupByExpression |= !transformResultMetadata.isSingleValue();
      hasNoDictionaryGroupByExpression |= !transformResultMetadata.hasDictionary();
    }
    _hasMVGroupByExpression = hasMVGroupByExpression;
    _numGroupByExpressions = groupByExpressions.length;
    _hasNoDictionaryGroupByExpression = hasNoDictionaryGroupByExpression;


    if (queryOptions != null) {
      Integer trimSize = QueryOptions.getMinSegmentGroupTrimSize(queryOptions);
      Integer threshold = QueryOptions.getMinSegmentGroupTrimThreshold(queryOptions);
      _onTheFlyTrimSize = trimSize != null ? trimSize : ON_THE_FLY_TRIM_SIZE;
      _onTheFlyTrimThreshold = threshold != null ? threshold : ON_THE_FLY_TRIM_THRESHOLD;
      _onTheFlyTrimFlag = _onTheFlyTrimThreshold > 0 && _onTheFlyTrimSize > 0;
    } else {
      _onTheFlyTrimSize = -1;
      _onTheFlyTrimThreshold = -1;
      _onTheFlyTrimFlag = false;
    }

    switch (keyGenType) {
      case noDictSingle:
        _groupKeyGenerator =
            new NoDictionarySingleColumnGroupKeyGenerator(transformOperator, groupByExpressions[0], numGroupsLimit);
        break;
      case NoDictMulti:
        _groupKeyGenerator =
            new NoDictionaryMultiColumnGroupKeyGenerator(transformOperator, groupByExpressions, numGroupsLimit);
        break;
      case DictArray:
      case DictIntMap:
      case DictLongMap:
      case DictArrayMap:
        _groupKeyGenerator = new DictionaryBasedGroupKeyGenerator(transformOperator, groupByExpressions, numGroupsLimit,
          maxInitialResultHolderCapacity, keyGenType);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported key gen type");
    }

    _tableResizer = tableResizer;

    // Initialize result holders
    int maxNumResults = _groupKeyGenerator.getGlobalGroupKeyUpperBound();
    int initialCapacity = Math.min(maxNumResults, maxInitialResultHolderCapacity);
    int numAggregationFunctions = aggregationFunctions.length;
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
    // No need to trim array type
    if (_onTheFlyTrimFlag && _groupKeyGenerator.getNumKeys() > _onTheFlyTrimThreshold
        && _groupKeyGenerator.getType() != DictArray) {
      PriorityQueue<DictIdRecord> pq;
      if (_hasNoDictionaryGroupByExpression) {
        // Use Different trim function based on key gen type
        if (_numGroupByExpressions == 1) {
          pq = _tableResizer.trimNoDictSingleCol(_groupKeyGenerator, _groupByResultHolders, _onTheFlyTrimSize);
        } else {
          pq = _tableResizer.trimNoDictMultiCol(_groupKeyGenerator, _groupByResultHolders, _onTheFlyTrimSize);
        }
      } else {
        pq = _tableResizer.trimDictCol(_groupKeyGenerator, _groupByResultHolders, _onTheFlyTrimSize);
      }

      _groupKeyGenerator.clearKeyHolder();
      // Clear result holders
      for (int i = 0; i < numAggregationFunctions; i++) {
        GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
        groupByResultHolder.clearResultHolder(capacityNeeded);
      }
      for (DictIdRecord record : pq) {
        // Get groupId
        int groupId = _groupKeyGenerator.getGroupId(record);
        for (int i = 0; i < numAggregationFunctions; i++) {
          Object value = record._record.getValues()[_numGroupByExpressions + i];
          GroupByResultHolder groupByResultHolder = _groupByResultHolders[i];
          if (groupByResultHolder.getType() == GroupByResultHolder.type.DOUBLE) {
            groupByResultHolder.setValueForKey(groupId, ((Number) value).doubleValue());
          } else {
            groupByResultHolder.setValueForKey(groupId, value);
          }
        }
      }
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
  public Collection<IntermediateRecord> trimGroupByResult(int trimSize) {
    return _tableResizer.trimInSegmentResults(_groupKeyGenerator.getGroupKeys(), _groupByResultHolders, trimSize);
  }
}
