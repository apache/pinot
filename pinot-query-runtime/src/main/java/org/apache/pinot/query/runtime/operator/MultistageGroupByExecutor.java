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
package org.apache.pinot.query.runtime.operator;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.function.BiFunction;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.groupby.GroupIdGenerator;
import org.apache.pinot.query.runtime.operator.groupby.GroupIdGeneratorFactory;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.utils.CommonConstants.Server;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/// Class that executes the keyed group by aggregations for the multistage AggregateOperator and MvAggregateOperator.
///
/// In the MV mode (non-empty `mvKeyIndexes`), the keys at those positions hold arrays. Each input row then
/// contributes to the group of every combination of its MV key elements, with the single-stage engine semantics: a
/// `null` value is one `null` element, an empty array gives no group, a repeated element counts once per copy, and
/// several MV keys give the cartesian product. The MV mode only accepts raw input (LEAF and DIRECT aggregations).
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultistageGroupByExecutor {
  private static final Object UNINITIALIZED_SORT_VALUE = new Object();

  private static final int[] NO_GROUP_IDS = new int[0];

  private final int[] _groupKeyIds;
  /// For each group key, whether it is an MV key. `null` when there is no MV key.
  @Nullable
  private final boolean[] _isMvKey;
  private final AggregationFunction[] _aggFunctions;
  private final int[] _filterArgIds;
  private final int _maxFilterArgId;
  private final AggType _aggType;
  private final boolean _leafReturnFinalResult;
  private final DataSchema _resultSchema;
  private int _rowsProcessed;
  private final int _numGroupsLimit;
  private final int _numGroupsWarningLimit;
  private final boolean _filteredAggregationsSkipEmptyGroups;

  // Group By Result holders for each mode
  private final GroupByResultHolder[] _aggregateResultHolders;
  private final List<Object[]> _mergeResultHolder;

  // Mapping from the row-key to a zero based integer index. This is used when we invoke the v1 aggregation functions
  // because they use the zero based integer indexes to store results.
  private final GroupIdGenerator _groupIdGenerator;

  public MultistageGroupByExecutor(int[] groupKeyIds, AggregationFunction[] aggFunctions, int[] filterArgIds,
      int maxFilterArgId, AggType aggType, boolean leafReturnFinalResult, DataSchema resultSchema,
      Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    this(groupKeyIds, new int[0], aggFunctions, filterArgIds, maxFilterArgId, aggType, leafReturnFinalResult,
        resultSchema, opChainMetadata, nodeHint);
  }

  /// @param mvKeyIndexes the positions in `groupKeyIds` of the MV keys, empty when there is no MV key
  public MultistageGroupByExecutor(int[] groupKeyIds, int[] mvKeyIndexes, AggregationFunction[] aggFunctions,
      int[] filterArgIds, int maxFilterArgId, AggType aggType, boolean leafReturnFinalResult, DataSchema resultSchema,
      Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    _groupKeyIds = groupKeyIds;
    if (mvKeyIndexes.length == 0) {
      _isMvKey = null;
    } else {
      Preconditions.checkState(!aggType.isInputIntermediateFormat(),
          "MV group keys are not supported with aggType: %s", aggType);
      _isMvKey = new boolean[groupKeyIds.length];
      for (int mvKeyIndex : mvKeyIndexes) {
        Preconditions.checkArgument(mvKeyIndex >= 0 && mvKeyIndex < groupKeyIds.length,
            "MV key index %s out of range for %s group keys", mvKeyIndex, groupKeyIds.length);
        _isMvKey[mvKeyIndex] = true;
      }
    }
    _aggFunctions = aggFunctions;
    _filterArgIds = filterArgIds;
    _maxFilterArgId = maxFilterArgId;
    _aggType = aggType;
    _leafReturnFinalResult = leafReturnFinalResult;
    _resultSchema = resultSchema;

    int maxInitialResultHolderCapacity = getResolvedMaxInitialResultHolderCapacity(opChainMetadata, nodeHint);

    _numGroupsLimit = getNumGroupsLimit(opChainMetadata, nodeHint);
    _numGroupsWarningLimit = getNumGroupsWarningLimit(opChainMetadata);

    // By default, we compute all groups for SQL compliant results. However, we allow overriding this behavior via
    // query option for improved performance.
    _filteredAggregationsSkipEmptyGroups = QueryOptionsUtils.isFilteredAggregationsSkipEmptyGroups(opChainMetadata);

    int numFunctions = aggFunctions.length;
    if (!aggType.isInputIntermediateFormat()) {
      _aggregateResultHolders = new GroupByResultHolder[numFunctions];
      for (int i = 0; i < numFunctions; i++) {
        _aggregateResultHolders[i] =
            _aggFunctions[i].createGroupByResultHolder(maxInitialResultHolderCapacity, _numGroupsLimit);
      }
      _mergeResultHolder = null;
    } else {
      _mergeResultHolder = new ArrayList<>(maxInitialResultHolderCapacity);
      _aggregateResultHolders = null;
    }

    _groupIdGenerator =
        GroupIdGeneratorFactory.getGroupIdGenerator(_resultSchema.getStoredColumnDataTypes(), groupKeyIds.length,
            _numGroupsLimit, maxInitialResultHolderCapacity);
  }

  private int getNumGroupsLimit(Map<String, String> opChainMetadata, @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> aggregateOptions = nodeHint.getHintOptions().get(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      if (aggregateOptions != null) {
        String numGroupsLimitStr = aggregateOptions.get(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT);
        if (numGroupsLimitStr != null) {
          return Integer.parseInt(numGroupsLimitStr);
        }
      }
    }
    Integer numGroupsLimit = QueryOptionsUtils.getNumGroupsLimit(opChainMetadata);
    return numGroupsLimit != null ? numGroupsLimit : Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_LIMIT;
  }

  private int getNumGroupsWarningLimit(Map<String, String> opChainMetadata) {
    Integer numGroupsWarningLimit = QueryOptionsUtils.getNumGroupsWarningLimit(opChainMetadata);
    return numGroupsWarningLimit != null ? numGroupsWarningLimit : Server.DEFAULT_QUERY_EXECUTOR_NUM_GROUPS_WARN_LIMIT;
  }

  private int getResolvedMaxInitialResultHolderCapacity(Map<String, String> opChainMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    Integer mseMaxInitialResultHolderCapacity = getMSEMaxInitialResultHolderCapacity(opChainMetadata, nodeHint);
    return (mseMaxInitialResultHolderCapacity != null) ? mseMaxInitialResultHolderCapacity
        : getMaxInitialResultHolderCapacity(opChainMetadata, nodeHint);
  }

  private int getMaxInitialResultHolderCapacity(Map<String, String> opChainMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> aggregateOptions = nodeHint.getHintOptions().get(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      if (aggregateOptions != null) {
        String maxInitialResultHolderCapacityStr =
            aggregateOptions.get(PinotHintOptions.AggregateOptions.MAX_INITIAL_RESULT_HOLDER_CAPACITY);
        if (maxInitialResultHolderCapacityStr != null) {
          return Integer.parseInt(maxInitialResultHolderCapacityStr);
        }
      }
    }
    Integer maxInitialResultHolderCapacity = QueryOptionsUtils.getMaxInitialResultHolderCapacity(opChainMetadata);
    return maxInitialResultHolderCapacity != null ? maxInitialResultHolderCapacity
        : Server.DEFAULT_QUERY_EXECUTOR_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  }

  private Integer getMSEMaxInitialResultHolderCapacity(Map<String, String> opChainMetadata,
      @Nullable PlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> aggregateOptions = nodeHint.getHintOptions().get(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      if (aggregateOptions != null) {
        String maxInitialMSEResultHolderCapacityStr =
            aggregateOptions.get(PinotHintOptions.AggregateOptions.MSE_MAX_INITIAL_RESULT_HOLDER_CAPACITY);
        if (maxInitialMSEResultHolderCapacityStr != null) {
          return Integer.parseInt(maxInitialMSEResultHolderCapacityStr);
        }
      }
    }
    // Don't return default value since null value means we need to fallback to MaxInitialResultHolderCapacity
    return QueryOptionsUtils.getMSEMaxInitialResultHolderCapacity(opChainMetadata);
  }

  public int getNumGroupsLimit() {
    return _numGroupsLimit;
  }

  public int getNumGroupsWarningLimit() {
    return _numGroupsWarningLimit;
  }

  /// Performs group-by aggregation for the data in the block.
  public void processBlock(MseBlock.Data block) {
    if (!_aggType.isInputIntermediateFormat()) {
      processAggregate(block);
    } else {
      processMerge(block);
    }
  }

  /// Select up to `maxRows` groups using `collations` and native final aggregate values for intermediate rows.
  public List<Object[]> getResult(List<RelFieldCollation> collations, int maxRows) {
    int numKeys = _groupKeyIds.length;
    int numCollations = collations.size();
    AggregationFunction[] sortFunctions = new AggregationFunction[numCollations];
    boolean finalizeSortValues = false;
    if (_aggType.isOutputIntermediateFormat() && !_leafReturnFinalResult) {
      if (_groupIdGenerator.getNumGroups() <= maxRows) {
        return getResult(maxRows);
      }
      for (int i = 0; i < numCollations; i++) {
        int index = collations.get(i).getFieldIndex();
        if (index < numKeys) {
          continue;
        }
        AggregationFunction aggFunction = _aggFunctions[index - numKeys];
        if (aggFunction.getIntermediateResultColumnType().getStoredType() != ColumnDataType.OBJECT) {
          continue;
        }
        sortFunctions[i] = aggFunction;
        finalizeSortValues = true;
      }
    }
    if (!finalizeSortValues) {
      return getResult(new SortUtils.SortComparator(collations, true), maxRows, (row, reusable) -> row,
          Function.identity());
    }
    SortUtils.SortComparator[] comparators = new SortUtils.SortComparator[numCollations];
    int[] sortColumnIndexes = new int[numCollations];
    for (int i = 0; i < numCollations; i++) {
      RelFieldCollation collation = collations.get(i);
      sortColumnIndexes[i] = collation.getFieldIndex();
      comparators[i] = new SortUtils.SortComparator(List.of(collation.withFieldIndex(i)), true);
    }
    Comparator<SortedRow> comparator = (r1, r2) -> {
      for (int i = 0; i < numCollations; i++) {
        int index = sortColumnIndexes[i];
        cacheSortValue(r1, i, index, sortFunctions[i]);
        cacheSortValue(r2, i, index, sortFunctions[i]);
        int result = comparators[i].compare(r1._sortKeys, r2._sortKeys);
        if (result != 0) {
          return result;
        }
      }
      return 0;
    };
    return getResult(comparator, maxRows, (row, reusable) -> getSortedRow(row, numCollations, reusable),
        row -> row._row);
  }

  /// Select groups with a shared heap loop; scalar rows use identity functions without allocating heap wrappers.
  private <T> List<Object[]> getResult(Comparator<T> comparator, int maxRows, BiFunction<Object[], T, T> decorate,
      Function<T, Object[]> unwrap) {
    int numGroups = Math.min(_groupIdGenerator.getNumGroups(), maxRows);
    if (numGroups == 0) {
      return List.of();
    }

    PriorityQueue<T> sortedRows = new PriorityQueue<>(numGroups, comparator);
    int numKeys = _groupKeyIds.length;
    int numFunctions = _aggFunctions.length;
    ColumnDataType[] resultStoredTypes = _resultSchema.getStoredColumnDataTypes();
    Iterator<GroupIdGenerator.GroupKey> groupKeyIterator =
        _groupIdGenerator.getGroupKeyIterator(numKeys + numFunctions);

    int idx = 0;
    while (idx < numGroups && groupKeyIterator.hasNext()) {
      T row = decorate.apply(getRow(groupKeyIterator, numKeys, numFunctions, resultStoredTypes), null);
      sortedRows.add(row);
      idx++;
    }

    T reusable = null;
    while (groupKeyIterator.hasNext()) {
      idx++;
      // TODO: allocate new array row only if row enters set
      T row = decorate.apply(getRow(groupKeyIterator, numKeys, numFunctions, resultStoredTypes), reusable);
      if (comparator.compare(sortedRows.peek(), row) < 0) {
        reusable = sortedRows.poll();
        sortedRows.offer(row);
      } else {
        reusable = row;
      }
    }

    _rowsProcessed = idx;

    int resultSize = sortedRows.size();
    ArrayList<Object[]> result = new ArrayList<>(sortedRows.size());
    for (int i = resultSize - 1; i >= 0; i--) {
      result.add(unwrap.apply(sortedRows.poll()));
    }
    // reverse priority queue order because comparators are reversed
    Collections.reverse(result);
    return result;
  }

  private SortedRow getSortedRow(Object[] row, int numCollations, @Nullable SortedRow reusable) {
    if (reusable == null) {
      reusable = new SortedRow(row, new Object[numCollations]);
    } else {
      reusable._row = row;
    }
    // Only rejected or evicted entries are reused; never change a row or cached key while it is in the heap.
    Arrays.fill(reusable._sortKeys, UNINITIALIZED_SORT_VALUE);
    return reusable;
  }

  private void cacheSortValue(SortedRow row, int sortIndex, int columnIndex,
      @Nullable AggregationFunction aggFunction) {
    if (row._sortKeys[sortIndex] == UNINITIALIZED_SORT_VALUE) {
      Object value = row._row[columnIndex];
      // Like SSE, compare native final objects before output conversion (e.g. BigDecimal, SerializedHLL).
      // Extract lazily and retain the intermediate row for merging. Cache null results as well as non-null results.
      row._sortKeys[sortIndex] = aggFunction != null ? aggFunction.extractFinalResult(value) : value;
    }
  }

  /// Query-local heap entry caching ORDER BY values. Its row and cache can be reset only outside the heap.
  private static final class SortedRow {
    private Object[] _row;
    private final Object[] _sortKeys;

    private SortedRow(Object[] row, Object[] sortKeys) {
      _row = row;
      _sortKeys = sortKeys;
    }
  }

  /// Get aggregation result limited to `maxRows` rows.
  public List<Object[]> getResult(int maxRows) {
    int numGroups = Math.min(_groupIdGenerator.getNumGroups(), maxRows);
    if (numGroups == 0) {
      return List.of();
    }

    List<Object[]> rows = new ArrayList<>(numGroups);
    int numKeys = _groupKeyIds.length;
    int numFunctions = _aggFunctions.length;
    ColumnDataType[] resultStoredTypes = _resultSchema.getStoredColumnDataTypes();
    Iterator<GroupIdGenerator.GroupKey> groupKeyIterator =
        _groupIdGenerator.getGroupKeyIterator(numKeys + numFunctions);

    int idx = 0;
    while (groupKeyIterator.hasNext() && idx < numGroups) {
      Object[] row = getRow(groupKeyIterator, numKeys, numFunctions, resultStoredTypes);
      rows.add(row);
      idx++;
    }
    if (groupKeyIterator.hasNext()) {
      _rowsProcessed = idx + 1;
    }
    return rows;
  }

  private Object[] getRow(Iterator<GroupIdGenerator.GroupKey> groupKeyIterator, int numKeys, int numFunctions,
      ColumnDataType[] resultStoredTypes) {
    GroupIdGenerator.GroupKey groupKey = groupKeyIterator.next();
    int groupId = groupKey._groupId;
    Object[] row = groupKey._row;
    int columnId = numKeys;
    for (int i = 0; i < numFunctions; i++) {
      row[columnId++] = getResultValue(i, groupId);
    }
    // Convert the results from AggregationFunction to the desired type
    TypeUtils.convertRow(row, resultStoredTypes);
    return row;
  }

  private Object getResultValue(int functionId, int groupId) {
    AggregationFunction aggFunction = _aggFunctions[functionId];
    switch (_aggType) {
      case LEAF: {
        Object intermediateResult = aggFunction.extractGroupByResult(_aggregateResultHolders[functionId], groupId);
        return _leafReturnFinalResult ? aggFunction.extractFinalResult(intermediateResult) : intermediateResult;
      }
      case INTERMEDIATE:
        return _mergeResultHolder.get(groupId)[functionId];
      case FINAL: {
        Object mergedResult = _mergeResultHolder.get(groupId)[functionId];
        return _leafReturnFinalResult ? mergedResult : aggFunction.extractFinalResult(mergedResult);
      }
      case DIRECT: {
        Object intermediateResult = aggFunction.extractGroupByResult(_aggregateResultHolders[functionId], groupId);
        return aggFunction.extractFinalResult(intermediateResult);
      }
      default:
        throw new IllegalStateException("Unsupported aggType: " + _aggType);
    }
  }

  public int getNumGroups() {
    return _groupIdGenerator.getNumGroups();
  }

  public int getRowsProcessed() {
    return _rowsProcessed;
  }

  public boolean isNumGroupsLimitReached() {
    return _groupIdGenerator.getNumGroups() == _numGroupsLimit;
  }

  private void processAggregate(MseBlock.Data block) {
    if (_isMvKey != null) {
      if (_maxFilterArgId < 0) {
        processMvAggregateWithoutFilter(block);
      } else {
        processMvAggregateWithFilter(block);
      }
    } else if (_maxFilterArgId < 0) {
      processAggregateWithoutFilter(block);
    } else {
      processAggregateWithFilter(block);
    }
  }

  private void processAggregateWithoutFilter(MseBlock.Data block) {
    int[] intKeys = generateGroupByKeys(block);
    int numGroups = _groupIdGenerator.getNumGroups();
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
      GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
      groupByResultHolder.ensureCapacity(numGroups);
      aggFunction.aggregateGroupBySV(block.getNumRows(), intKeys, groupByResultHolder, blockValSetMap);
    }
  }

  private void processAggregateWithFilter(MseBlock.Data block) {
    // In the first loop, generate all the group keys, cache the matching rows
    int[] intKeys = _filteredAggregationsSkipEmptyGroups ? null : generateGroupByKeys(block);
    RoaringBitmap[] matchedBitmaps = new RoaringBitmap[_maxFilterArgId + 1];
    int[] numMatchedRowsArray = new int[_maxFilterArgId + 1];
    int[][] filteredIntKeysArray = new int[_maxFilterArgId + 1][];
    for (int filterArgId : _filterArgIds) {
      if (filterArgId < 0) {
        // No filter for this aggregation function
        if (intKeys == null) {
          intKeys = generateGroupByKeys(block);
        }
      } else {
        // Need to filter the block before aggregation
        if (matchedBitmaps[filterArgId] == null) {
          RoaringBitmap matchedBitmap = AggregateOperator.getMatchedBitmap(block, filterArgId);
          matchedBitmaps[filterArgId] = matchedBitmap;
          int numMatchedRows = matchedBitmap.getCardinality();
          numMatchedRowsArray[filterArgId] = numMatchedRows;
          filteredIntKeysArray[filterArgId] = generateGroupByKeys(block, numMatchedRows, matchedBitmap);
        }
      }
    }

    // In the second loop, aggregate the values
    int numGroups = _groupIdGenerator.getNumGroups();
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
      groupByResultHolder.ensureCapacity(numGroups);
      int filterArgId = _filterArgIds[i];
      if (filterArgId < 0) {
        Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
        aggFunction.aggregateGroupBySV(block.getNumRows(), intKeys, groupByResultHolder, blockValSetMap);
      } else {
        Map<ExpressionContext, BlockValSet> blockValSetMap =
            AggregateOperator.getFilteredBlockValSetMap(aggFunction, block, numMatchedRowsArray[filterArgId],
                matchedBitmaps[filterArgId]);
        aggFunction.aggregateGroupBySV(numMatchedRowsArray[filterArgId], filteredIntKeysArray[filterArgId],
            groupByResultHolder, blockValSetMap);
      }
    }
  }

  private void processMerge(MseBlock.Data block) {
    int[] groupByKeys = generateGroupByKeys(block);
    int numRows = groupByKeys.length;
    int numFunctions = _aggFunctions.length;
    Object[][] intermediateResults = new Object[numFunctions][];
    // Serialized custom object intermediate results are merged straight from the data block (see mergeSerialized).
    DataBlock[] serializedResults = new DataBlock[numFunctions];
    int[] serializedResultColIds = new int[numFunctions];
    for (int i = 0; i < numFunctions; i++) {
      if (!_leafReturnFinalResult) {
        serializedResults[i] = AggregateOperator.getSerializedIntermediateResults(_aggFunctions[i], block);
      }
      if (serializedResults[i] != null) {
        serializedResultColIds[i] = AggregateOperator.getIntermediateResultColId(_aggFunctions[i]);
      } else {
        intermediateResults[i] = AggregateOperator.getIntermediateResults(_aggFunctions[i], block);
      }
    }
    if (_leafReturnFinalResult) {
      for (int i = 0; i < numRows; i++) {
        int groupByKey = groupByKeys[i];
        if (groupByKey == GroupKeyGenerator.INVALID_ID) {
          continue;
        }
        Object[] mergedResults;
        if (_mergeResultHolder.size() == groupByKey) {
          mergedResults = new Object[numFunctions];
          _mergeResultHolder.add(mergedResults);
        } else {
          mergedResults = _mergeResultHolder.get(groupByKey);
        }
        for (int j = 0; j < numFunctions; j++) {
          mergedResults[j] =
              AggregationFunctionUtils.mergeFinalResult(_aggFunctions[j], mergedResults[j], intermediateResults[j][i]);
        }
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        int groupByKey = groupByKeys[i];
        if (groupByKey == GroupKeyGenerator.INVALID_ID) {
          continue;
        }
        Object[] mergedResults;
        if (_mergeResultHolder.size() == groupByKey) {
          mergedResults = new Object[numFunctions];
          _mergeResultHolder.add(mergedResults);
        } else {
          mergedResults = _mergeResultHolder.get(groupByKey);
        }
        for (int j = 0; j < numFunctions; j++) {
          DataBlock serialized = serializedResults[j];
          mergedResults[j] = serialized != null
              ? AggregationFunctionUtils.mergeSerialized(_aggFunctions[j], mergedResults[j],
                  serialized.getCustomObject(i, serializedResultColIds[j]))
              : AggregationFunctionUtils.merge(_aggFunctions[j], mergedResults[j], intermediateResults[j][i]);
        }
      }
    }
  }

  /// Creates the group by key for each row. Converts the key into a 0-index based int value that can be used by
  /// GroupByAggregationResultHolders used in v1 aggregations.
  private int[] generateGroupByKeys(MseBlock.Data block) {
    return block.isRowHeap()
        ? generateGroupByKeys(block.asRowHeap().getRows())
        : generateGroupByKeys(block.asSerialized().getDataBlock());
  }

  private int[] generateGroupByKeys(List<Object[]> rows) {
    int numRows = rows.size();
    int[] intKeys = new int[numRows];
    int numKeys = _groupKeyIds.length;
    if (numKeys == 1) {
      int groupKeyId = _groupKeyIds[0];
      for (int i = 0; i < numRows; i++) {
        intKeys[i] = _groupIdGenerator.getGroupId(rows.get(i)[groupKeyId]);
      }
    } else {
      Object[] key = new Object[numKeys];
      for (int i = 0; i < numRows; i++) {
        Object[] row = rows.get(i);
        for (int j = 0; j < numKeys; j++) {
          key[j] = row[_groupKeyIds[j]];
        }
        intKeys[i] = _groupIdGenerator.getGroupId(key);
      }
    }
    return intKeys;
  }

  private int[] generateGroupByKeys(DataBlock dataBlock) {
    int numRows = dataBlock.getNumberOfRows();
    int[] intKeys = new int[numRows];
    int numKeys = _groupKeyIds.length;
    if (numKeys == 1) {
      Object[] keys = DataBlockExtractUtils.extractKey(dataBlock, _groupKeyIds[0]);
      for (int i = 0; i < numRows; i++) {
        intKeys[i] = _groupIdGenerator.getGroupId(keys[i]);
      }
    } else {
      Object[][] columns = new Object[numKeys][];
      for (int i = 0; i < numKeys; i++) {
        columns[i] = DataBlockExtractUtils.extractKey(dataBlock, _groupKeyIds[i]);
      }
      Object[] key = new Object[numKeys];
      for (int rowId = 0; rowId < numRows; rowId++) {
        for (int i = 0; i < numKeys; i++) {
          key[i] = columns[i][rowId];
        }
        intKeys[rowId] = _groupIdGenerator.getGroupId(key);
      }
    }
    return intKeys;
  }

  /// Creates the group by key for each row. Converts the key into a 0-index based int value that can be used by
  /// GroupByAggregationResultHolders used in v1 aggregations.
  private int[] generateGroupByKeys(MseBlock.Data block, int numMatchedRows, RoaringBitmap matchedBitmap) {
    return block.isRowHeap()
        ? generateGroupByKeys(block.asRowHeap().getRows(), numMatchedRows, matchedBitmap)
        : generateGroupByKeys(block.asSerialized().getDataBlock(), numMatchedRows, matchedBitmap);
  }

  private int[] generateGroupByKeys(List<Object[]> rows, int numMatchedRows, RoaringBitmap matchedBitmap) {
    int[] intKeys = new int[numMatchedRows];
    int numKeys = _groupKeyIds.length;
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    if (numKeys == 1) {
      int groupKeyId = _groupKeyIds[0];
      for (int i = 0; i < numMatchedRows; i++) {
        intKeys[i] = _groupIdGenerator.getGroupId(rows.get(iterator.next())[groupKeyId]);
      }
    } else {
      Object[] key = new Object[numKeys];
      for (int i = 0; i < numMatchedRows; i++) {
        int rowId = iterator.next();
        Object[] row = rows.get(rowId);
        for (int j = 0; j < numKeys; j++) {
          key[j] = row[_groupKeyIds[j]];
        }
        intKeys[i] = _groupIdGenerator.getGroupId(key);
      }
    }
    return intKeys;
  }

  private int[] generateGroupByKeys(DataBlock dataBlock, int numMatchedRows, RoaringBitmap matchedBitmap) {
    int[] intKeys = new int[numMatchedRows];
    int numKeys = _groupKeyIds.length;
    if (numKeys == 1) {
      Object[] keys = DataBlockExtractUtils.extractKey(dataBlock, _groupKeyIds[0], numMatchedRows, matchedBitmap);
      for (int i = 0; i < numMatchedRows; i++) {
        intKeys[i] = _groupIdGenerator.getGroupId(keys[i]);
      }
    } else {
      Object[][] columns = new Object[numKeys][];
      for (int i = 0; i < numKeys; i++) {
        columns[i] = DataBlockExtractUtils.extractKey(dataBlock, _groupKeyIds[i], numMatchedRows, matchedBitmap);
      }
      Object[] key = new Object[numKeys];
      for (int rowId = 0; rowId < numMatchedRows; rowId++) {
        for (int i = 0; i < numKeys; i++) {
          key[i] = columns[i][rowId];
        }
        intKeys[rowId] = _groupIdGenerator.getGroupId(key);
      }
    }
    return intKeys;
  }

  private void processMvAggregateWithoutFilter(MseBlock.Data block) {
    int[][] mvKeys = generateMvGroupByKeys(block, null);
    int numGroups = _groupIdGenerator.getNumGroups();
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
      GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
      groupByResultHolder.ensureCapacity(numGroups);
      aggFunction.aggregateGroupByMV(block.getNumRows(), mvKeys, groupByResultHolder, blockValSetMap);
    }
  }

  /// The MV counterpart of [#processAggregateWithFilter(MseBlock.Data)]: a filtered aggregation receives the group
  /// ids of the matched rows only, in matched order.
  private void processMvAggregateWithFilter(MseBlock.Data block) {
    // In the first loop, generate all the group keys, cache the matching rows
    int[][] mvKeys = _filteredAggregationsSkipEmptyGroups ? null : generateMvGroupByKeys(block, null);
    RoaringBitmap[] matchedBitmaps = new RoaringBitmap[_maxFilterArgId + 1];
    int[] numMatchedRowsArray = new int[_maxFilterArgId + 1];
    int[][][] filteredMvKeysArray = new int[_maxFilterArgId + 1][][];
    for (int filterArgId : _filterArgIds) {
      if (filterArgId < 0) {
        // No filter for this aggregation function
        if (mvKeys == null) {
          mvKeys = generateMvGroupByKeys(block, null);
        }
      } else {
        // Need to filter the block before aggregation
        if (matchedBitmaps[filterArgId] == null) {
          RoaringBitmap matchedBitmap = AggregateOperator.getMatchedBitmap(block, filterArgId);
          matchedBitmaps[filterArgId] = matchedBitmap;
          numMatchedRowsArray[filterArgId] = matchedBitmap.getCardinality();
          filteredMvKeysArray[filterArgId] = generateMvGroupByKeys(block, matchedBitmap);
        }
      }
    }

    // In the second loop, aggregate the values
    int numGroups = _groupIdGenerator.getNumGroups();
    for (int i = 0; i < _aggFunctions.length; i++) {
      AggregationFunction aggFunction = _aggFunctions[i];
      GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
      groupByResultHolder.ensureCapacity(numGroups);
      int filterArgId = _filterArgIds[i];
      if (filterArgId < 0) {
        Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
        aggFunction.aggregateGroupByMV(block.getNumRows(), mvKeys, groupByResultHolder, blockValSetMap);
      } else {
        Map<ExpressionContext, BlockValSet> blockValSetMap =
            AggregateOperator.getFilteredBlockValSetMap(aggFunction, block, numMatchedRowsArray[filterArgId],
                matchedBitmaps[filterArgId]);
        aggFunction.aggregateGroupByMV(numMatchedRowsArray[filterArgId], filteredMvKeysArray[filterArgId],
            groupByResultHolder, blockValSetMap);
      }
    }
  }

  /// Creates the group ids of each row in MV mode: one id per combination of the row's MV key elements.
  ///
  /// @param matchedBitmap the rows to read, or `null` for every row of the block
  /// @return for each selected row, in order, its group ids ([GroupIdGenerator#INVALID_ID] when the groups limit is
  ///         reached), or an empty array when one of its MV keys is an empty array
  private int[][] generateMvGroupByKeys(MseBlock.Data block, @Nullable RoaringBitmap matchedBitmap) {
    int numKeys = _groupKeyIds.length;
    int numRows = matchedBitmap == null ? block.getNumRows() : matchedBitmap.getCardinality();
    Object[][] columns = new Object[numKeys][];
    if (block.isRowHeap()) {
      List<Object[]> rows = block.asRowHeap().getRows();
      for (int i = 0; i < numKeys; i++) {
        columns[i] = new Object[numRows];
      }
      if (matchedBitmap == null) {
        for (int rowId = 0; rowId < numRows; rowId++) {
          Object[] row = rows.get(rowId);
          for (int i = 0; i < numKeys; i++) {
            columns[i][rowId] = row[_groupKeyIds[i]];
          }
        }
      } else {
        PeekableIntIterator iterator = matchedBitmap.getIntIterator();
        for (int matchedRowId = 0; matchedRowId < numRows; matchedRowId++) {
          Object[] row = rows.get(iterator.next());
          for (int i = 0; i < numKeys; i++) {
            columns[i][matchedRowId] = row[_groupKeyIds[i]];
          }
        }
      }
    } else {
      DataBlock dataBlock = block.asSerialized().getDataBlock();
      for (int i = 0; i < numKeys; i++) {
        columns[i] = matchedBitmap == null
            ? DataBlockExtractUtils.extractKey(dataBlock, _groupKeyIds[i])
            : DataBlockExtractUtils.extractKey(dataBlock, _groupKeyIds[i], numRows, matchedBitmap);
      }
    }

    int[][] mvKeys = new int[numRows][];
    Object[][] elements = new Object[numKeys][];
    Object[] key = numKeys > 1 ? new Object[numKeys] : null;
    for (int rowId = 0; rowId < numRows; rowId++) {
      mvKeys[rowId] = getMvGroupIds(columns, rowId, elements, key);
    }
    return mvKeys;
  }

  private int countMvKeys() {
    int numMvKeys = 0;
    for (boolean isMvKey : _isMvKey) {
      if (isMvKey) {
        numMvKeys++;
      }
    }
    return numMvKeys;
  }

  /// Returns the group ids of one row. The combinations are generated as in the single-stage engine
  /// (`DictionaryBasedGroupKeyGenerator.getIntRawKeys`): the last key varies fastest, the first key slowest. This order
  /// is an implementation detail, not a contract: without ORDER BY, the groups that survive the groups limit are not
  /// defined.
  ///
  /// @param elements scratch storage for the elements of each MV key
  /// @param key scratch storage for a multi-column key, `null` when there is one key
  private int[] getMvGroupIds(Object[][] columns, int rowId, Object[][] elements, @Nullable Object[] key) {
    int numKeys = columns.length;
    int numCombinations = 1;
    for (int i = 0; i < numKeys; i++) {
      if (_isMvKey[i]) {
        Object[] keyElements = MvKeyExpander.elements(columns[i][rowId]);
        if (keyElements.length == 0) {
          return NO_GROUP_IDS;
        }
        elements[i] = keyElements;
        try {
          numCombinations = Math.multiplyExact(numCombinations, keyElements.length);
        } catch (ArithmeticException e) {
          throw QueryErrorCode.SERVER_RESOURCE_LIMIT_EXCEEDED.asException(
              "Too many MV group key combinations in one row: more than " + Integer.MAX_VALUE + " for "
                  + countMvKeys() + " MV keys", e);
        }
      }
    }
    int[] groupIds = new int[numCombinations];
    if (key == null) {
      // A single key, which is the MV key
      Object[] keyElements = elements[0];
      for (int j = 0; j < numCombinations; j++) {
        groupIds[j] = _groupIdGenerator.getGroupId(keyElements[j]);
      }
      return groupIds;
    }
    for (int j = 0; j < numCombinations; j++) {
      int remainder = j;
      for (int i = numKeys - 1; i >= 0; i--) {
        if (_isMvKey[i]) {
          Object[] keyElements = elements[i];
          int numElements = keyElements.length;
          key[i] = keyElements[remainder % numElements];
          remainder /= numElements;
        } else {
          key[i] = columns[i][rowId];
        }
      }
      groupIds[j] = _groupIdGenerator.getGroupId(key);
    }
    return groupIds;
  }
}
