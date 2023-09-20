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

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.data.table.Key;
import org.apache.pinot.core.plan.maker.InstancePlanMakerImplV2;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupKeyGenerator;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.AggregateNode.AggType;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.TypeUtils;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * Class that executes the group by aggregations for the multistage AggregateOperator.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class MultistageGroupByExecutor {
  private final int[] _groupKeyIds;
  private final AggregationFunction[] _aggFunctions;
  private final int[] _filterArgIds;
  private final int _maxFilterArgId;
  private final AggType _aggType;
  private final DataSchema _resultSchema;
  private final int _numGroupsLimit;

  // Group By Result holders for each mode
  private final GroupByResultHolder[] _aggregateResultHolders;
  private final List<Object[]> _mergeResultHolder;

  // Mapping from the row-key to a zero based integer index. This is used when we invoke the v1 aggregation functions
  // because they use the zero based integer indexes to store results.
  private final Object2IntOpenHashMap<Object> _groupKeyToIdMap;

  public MultistageGroupByExecutor(int[] groupKeyIds, AggregationFunction[] aggFunctions, int[] filterArgIds,
      int maxFilterArgId, AggType aggType, DataSchema resultSchema, Map<String, String> opChainMetadata,
      @Nullable AbstractPlanNode.NodeHint nodeHint) {
    _groupKeyIds = groupKeyIds;
    _aggFunctions = aggFunctions;
    _filterArgIds = filterArgIds;
    _maxFilterArgId = maxFilterArgId;
    _aggType = aggType;
    _resultSchema = resultSchema;
    int maxInitialResultHolderCapacity = getMaxInitialResultHolderCapacity(opChainMetadata, nodeHint);
    _numGroupsLimit = getNumGroupsLimit(opChainMetadata, nodeHint);

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

    _groupKeyToIdMap = new Object2IntOpenHashMap<>();
    _groupKeyToIdMap.defaultReturnValue(GroupKeyGenerator.INVALID_ID);
  }

  private int getNumGroupsLimit(Map<String, String> customProperties, @Nullable AbstractPlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> aggregateOptions = nodeHint._hintOptions.get(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      if (aggregateOptions != null) {
        String numGroupsLimitStr = aggregateOptions.get(PinotHintOptions.AggregateOptions.NUM_GROUPS_LIMIT);
        if (numGroupsLimitStr != null) {
          return Integer.parseInt(numGroupsLimitStr);
        }
      }
    }
    Integer numGroupsLimit = QueryOptionsUtils.getNumGroupsLimit(customProperties);
    return numGroupsLimit != null ? numGroupsLimit : InstancePlanMakerImplV2.DEFAULT_NUM_GROUPS_LIMIT;
  }

  private int getMaxInitialResultHolderCapacity(Map<String, String> customProperties,
      @Nullable AbstractPlanNode.NodeHint nodeHint) {
    if (nodeHint != null) {
      Map<String, String> aggregateOptions = nodeHint._hintOptions.get(PinotHintOptions.AGGREGATE_HINT_OPTIONS);
      if (aggregateOptions != null) {
        String maxInitialResultHolderCapacityStr =
            aggregateOptions.get(PinotHintOptions.AggregateOptions.MAX_INITIAL_RESULT_HOLDER_CAPACITY);
        if (maxInitialResultHolderCapacityStr != null) {
          return Integer.parseInt(maxInitialResultHolderCapacityStr);
        }
      }
    }
    Integer maxInitialResultHolderCapacity = QueryOptionsUtils.getMaxInitialResultHolderCapacity(customProperties);
    return maxInitialResultHolderCapacity != null ? maxInitialResultHolderCapacity
        : InstancePlanMakerImplV2.DEFAULT_MAX_INITIAL_RESULT_HOLDER_CAPACITY;
  }

  public int getNumGroupsLimit() {
    return _numGroupsLimit;
  }

  /**
   * Performs group-by aggregation for the data in the block.
   */
  public void processBlock(TransferableBlock block) {
    if (!_aggType.isInputIntermediateFormat()) {
      processAggregate(block);
    } else {
      processMerge(block);
    }
  }

  /**
   * Fetches the result.
   */
  public List<Object[]> getResult() {
    if (_groupKeyToIdMap.isEmpty()) {
      return Collections.emptyList();
    }
    List<Object[]> rows = new ArrayList<>(_groupKeyToIdMap.size());
    int numKeys = _groupKeyIds.length;
    int numFunctions = _aggFunctions.length;
    int numColumns = numKeys + numFunctions;
    ColumnDataType[] resultStoredTypes = _resultSchema.getStoredColumnDataTypes();
    if (numKeys == 1) {
      for (Object2IntMap.Entry<Object> entry : _groupKeyToIdMap.object2IntEntrySet()) {
        Object[] row = new Object[numColumns];
        row[0] = entry.getKey();
        int groupId = entry.getIntValue();
        for (int i = 0; i < numFunctions; i++) {
          row[i + 1] = getResultValue(i, groupId);
        }
        // Convert the results from AggregationFunction to the desired type
        TypeUtils.convertRow(row, resultStoredTypes);
        rows.add(row);
      }
    } else {
      for (Object2IntMap.Entry<Object> entry : _groupKeyToIdMap.object2IntEntrySet()) {
        Object[] row = new Object[numColumns];
        Object[] keyValues = ((Key) entry.getKey()).getValues();
        System.arraycopy(keyValues, 0, row, 0, numKeys);
        int groupId = entry.getIntValue();
        for (int i = 0; i < numFunctions; i++) {
          row[numKeys + i] = getResultValue(i, groupId);
        }
        // Convert the results from AggregationFunction to the desired type
        TypeUtils.convertRow(row, resultStoredTypes);
        rows.add(row);
      }
    }
    return rows;
  }

  private Object getResultValue(int functionId, int groupId) {
    AggregationFunction aggFunction = _aggFunctions[functionId];
    switch (_aggType) {
      case LEAF:
        return aggFunction.extractGroupByResult(_aggregateResultHolders[functionId], groupId);
      case INTERMEDIATE:
        return _mergeResultHolder.get(groupId)[functionId];
      case FINAL:
        return aggFunction.extractFinalResult(_mergeResultHolder.get(groupId)[functionId]);
      case DIRECT:
        Object intermediate = aggFunction.extractGroupByResult(_aggregateResultHolders[functionId], groupId);
        return aggFunction.extractFinalResult(intermediate);
      default:
        throw new IllegalStateException("Unsupported aggType: " + _aggType);
    }
  }

  public boolean isNumGroupsLimitReached() {
    return _groupKeyToIdMap.size() == _numGroupsLimit;
  }

  private void processAggregate(TransferableBlock block) {
    if (_maxFilterArgId < 0) {
      // No filter for any aggregation function
      int[] intKeys = generateGroupByKeys(block);
      for (int i = 0; i < _aggFunctions.length; i++) {
        AggregationFunction aggFunction = _aggFunctions[i];
        Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
        GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
        groupByResultHolder.ensureCapacity(_groupKeyToIdMap.size());
        aggFunction.aggregateGroupBySV(block.getNumRows(), intKeys, groupByResultHolder, blockValSetMap);
      }
    } else {
      // Some aggregation functions have filter, cache the matching rows
      int[] intKeys = null;
      RoaringBitmap[] matchedBitmaps = new RoaringBitmap[_maxFilterArgId + 1];
      int[] numMatchedRowsArray = new int[_maxFilterArgId + 1];
      int[][] filteredIntKeysArray = new int[_maxFilterArgId + 1][];
      for (int i = 0; i < _aggFunctions.length; i++) {
        AggregationFunction aggFunction = _aggFunctions[i];
        int filterArgId = _filterArgIds[i];
        if (filterArgId < 0) {
          // No filter for this aggregation function
          if (intKeys == null) {
            intKeys = generateGroupByKeys(block);
          }
          Map<ExpressionContext, BlockValSet> blockValSetMap = AggregateOperator.getBlockValSetMap(aggFunction, block);
          GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
          groupByResultHolder.ensureCapacity(_groupKeyToIdMap.size());
          aggFunction.aggregateGroupBySV(block.getNumRows(), intKeys, groupByResultHolder, blockValSetMap);
        } else {
          // Need to filter the block before aggregation
          RoaringBitmap matchedBitmap = matchedBitmaps[filterArgId];
          if (matchedBitmap == null) {
            matchedBitmap = AggregateOperator.getMatchedBitmap(block, filterArgId);
            matchedBitmaps[filterArgId] = matchedBitmap;
            int numMatchedRows = matchedBitmap.getCardinality();
            numMatchedRowsArray[filterArgId] = numMatchedRows;
            filteredIntKeysArray[filterArgId] = generateGroupByKeys(block, numMatchedRows, matchedBitmap);
          }
          int numMatchedRows = numMatchedRowsArray[filterArgId];
          int[] filteredIntKeys = filteredIntKeysArray[filterArgId];
          Map<ExpressionContext, BlockValSet> blockValSetMap =
              AggregateOperator.getFilteredBlockValSetMap(aggFunction, block, numMatchedRows, matchedBitmap);
          GroupByResultHolder groupByResultHolder = _aggregateResultHolders[i];
          groupByResultHolder.ensureCapacity(_groupKeyToIdMap.size());
          aggFunction.aggregateGroupBySV(numMatchedRows, filteredIntKeys, groupByResultHolder, blockValSetMap);
        }
      }
    }
  }

  private void processMerge(TransferableBlock block) {
    int[] groupByKeys = generateGroupByKeys(block);
    int numRows = groupByKeys.length;
    int numFunctions = _aggFunctions.length;
    Object[][] intermediateResults = new Object[numFunctions][numRows];
    for (int i = 0; i < numFunctions; i++) {
      intermediateResults[i] = AggregateOperator.getIntermediateResults(_aggFunctions[i], block);
    }
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
        AggregationFunction aggFunction = _aggFunctions[j];
        Object intermediateResult = intermediateResults[j][i];
        // Not all V1 aggregation functions have null-handling logic. Handle null values before calling merge.
        // TODO: Fix it
        if (intermediateResult == null) {
          continue;
        }
        if (mergedResults[j] == null) {
          mergedResults[j] = intermediateResult;
        } else {
          mergedResults[j] = aggFunction.merge(mergedResults[j], intermediateResult);
        }
      }
    }
  }

  /**
   * Creates the group by key for each row. Converts the key into a 0-index based int value that can be used by
   * GroupByAggregationResultHolders used in v1 aggregations.
   */
  private int[] generateGroupByKeys(TransferableBlock block) {
    return block.isContainerConstructed() ? generateGroupByKeys(block.getContainer())
        : generateGroupByKeys(block.getDataBlock());
  }

  private int[] generateGroupByKeys(List<Object[]> rows) {
    int numRows = rows.size();
    int[] intKeys = new int[numRows];
    int numKeys = _groupKeyIds.length;
    if (numKeys == 1) {
      int groupKeyId = _groupKeyIds[0];
      for (int i = 0; i < numRows; i++) {
        intKeys[i] = getGroupId(rows.get(i)[groupKeyId]);
      }
    } else {
      for (int i = 0; i < numRows; i++) {
        Object[] row = rows.get(i);
        Object[] keyValues = new Object[numKeys];
        for (int j = 0; j < numKeys; j++) {
          keyValues[j] = row[_groupKeyIds[j]];
        }
        intKeys[i] = getGroupId(new Key(keyValues));
      }
    }
    return intKeys;
  }

  private int[] generateGroupByKeys(DataBlock dataBlock) {
    Object[] keys;
    if (_groupKeyIds.length == 1) {
      keys = DataBlockExtractUtils.extractColumn(dataBlock, _groupKeyIds[0]);
    } else {
      keys = DataBlockExtractUtils.extractKeys(dataBlock, _groupKeyIds);
    }
    int numRows = keys.length;
    int[] intKeys = new int[numRows];
    for (int i = 0; i < numRows; i++) {
      intKeys[i] = getGroupId(keys[i]);
    }
    return intKeys;
  }

  /**
   * Creates the group by key for each row. Converts the key into a 0-index based int value that can be used by
   * GroupByAggregationResultHolders used in v1 aggregations.
   */
  private int[] generateGroupByKeys(TransferableBlock block, int numMatchedRows, RoaringBitmap matchedBitmap) {
    return block.isContainerConstructed() ? generateGroupByKeys(block.getContainer(), numMatchedRows, matchedBitmap)
        : generateGroupByKeys(block.getDataBlock(), numMatchedRows, matchedBitmap);
  }

  private int[] generateGroupByKeys(List<Object[]> rows, int numMatchedRows, RoaringBitmap matchedBitmap) {
    int[] intKeys = new int[numMatchedRows];
    int numKeys = _groupKeyIds.length;
    PeekableIntIterator iterator = matchedBitmap.getIntIterator();
    if (numKeys == 1) {
      int groupKeyId = _groupKeyIds[0];
      for (int i = 0; i < numMatchedRows; i++) {
        intKeys[i] = getGroupId(rows.get(iterator.next())[groupKeyId]);
      }
    } else {
      for (int i = 0; i < numMatchedRows; i++) {
        int rowId = iterator.next();
        Object[] row = rows.get(rowId);
        Object[] keyValues = new Object[numKeys];
        for (int j = 0; j < numKeys; j++) {
          keyValues[j] = row[_groupKeyIds[j]];
        }
        intKeys[i] = getGroupId(new Key(keyValues));
      }
    }
    return intKeys;
  }

  private int[] generateGroupByKeys(DataBlock dataBlock, int numMatchedRows, RoaringBitmap matchedBitmap) {
    Object[] keys;
    if (_groupKeyIds.length == 1) {
      keys = DataBlockExtractUtils.extractColumn(dataBlock, _groupKeyIds[0], numMatchedRows, matchedBitmap);
    } else {
      keys = DataBlockExtractUtils.extractKeys(dataBlock, _groupKeyIds, numMatchedRows, matchedBitmap);
    }
    int[] intKeys = new int[numMatchedRows];
    for (int i = 0; i < numMatchedRows; i++) {
      intKeys[i] = getGroupId(keys[i]);
    }
    return intKeys;
  }

  private int getGroupId(Object key) {
    int numGroups = _groupKeyToIdMap.size();
    if (numGroups < _numGroupsLimit) {
      return _groupKeyToIdMap.computeIntIfAbsent(key, k -> numGroups);
    } else {
      return _groupKeyToIdMap.getInt(key);
    }
  }
}
