/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.operator.query;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.DocIdSetBlock;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.query.aggregation.groupby.BitHacks;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * This GroupBy Operator takes all the required dataSources. For groupBy columns,
 * it creates a long value as group key instead of a String.
 * This will make the algorithm performs better.
 *
 * GetAggregationGroupByResult will return the results.
 *
 *
 */
public class MAggregationFunctionGroupByWithDictionaryOperator extends AggregationFunctionGroupByOperator {

  private int[] _groupKeyBitSize;
  private final String[] _stringArray;

  private final Dictionary[] _dictionaries;
  private final BlockValIterator[] _groupByBlockValIterators;

  private final Long2ObjectOpenHashMap<Serializable> _tempAggregationResults =
      new Long2ObjectOpenHashMap<Serializable>();

  public MAggregationFunctionGroupByWithDictionaryOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator, boolean hasDictionary) {
    super(aggregationInfo, groupBy, projectionOperator, hasDictionary);
    _dictionaries = new Dictionary[_groupBy.getColumnsSize()];
    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];
    setGroupKeyOffset();
    _stringArray = new String[_groupKeyBitSize.length];
  }

  private void setGroupKeyOffset() {
    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      _dictionaries[i] = _groupByBlocks[i].getMetadata().getDictionary();
    }
    _groupKeyBitSize = new int[_groupBy.getColumnsSize()];
    int totalBitSet = 0;
    for (int i = 0; i < _groupBy.getColumnsSize(); i++) {
      _groupKeyBitSize[i] = BitHacks.findLogBase2(_dictionaries[i].length()) + 1;
      totalBitSet += _groupKeyBitSize[i];
    }
    if (totalBitSet > 64) {
      throw new IllegalArgumentException("Too many columns for an efficient group by");
    }
  }

  @Override
  public Block getNextBlock() {
    final ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
    if (block == null) {
      return null;
    }
    DocIdSetBlock docIdSetBlock = (DocIdSetBlock) block.getDocIdSetBlock();
    BlockDocIdIterator blockDocIdIterator = docIdSetBlock.getBlockDocIdSet().iterator();
    int docId = 0;

    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      _groupByBlockValIterators[i] = block.getBlock(_groupBy.getColumns().get(i)).getBlockValueSet().iterator();
    }

    while ((docId = blockDocIdIterator.next()) != Constants.EOF) {
      if (!_isGroupByColumnsContainMultiValueColumn) {
        final long groupKey = getGroupKey(docId);
        _tempAggregationResults.put(groupKey,
            _aggregationFunction.aggregate(_tempAggregationResults.get(groupKey), docId, _aggregationFunctionBlocks));

      } else {
        for (long groupKey : getGroupKeys(docId)) {
          _tempAggregationResults.put(groupKey,
              _aggregationFunction.aggregate(_tempAggregationResults.get(groupKey), docId, _aggregationFunctionBlocks));
        }
      }
    }
    return null;
  }

  private Long[] getGroupKeys(int docId) {
    List<Long> groupKeysList = new ArrayList<Long>();
    groupKeysList.add(0L);
    int i = 0;
    for (final int element : _groupKeyBitSize) {
      if (_isSingleValueGroupByColumn[i]) {
        BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
        blockValIterator.skipTo(docId);
        int dictId = blockValIterator.nextIntVal();
        for (int j = 0; j < groupKeysList.size(); ++j) {
          groupKeysList.set(j, (groupKeysList.get(j) << element) | dictId);
        }
      } else {
        BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[i];
        blockValIterator.skipTo(docId);
        final int maxValue = _groupByBlocks[i].getMetadata().getMaxNumberOfMultiValues();
        final int[] entries = new int[maxValue];
        int group = blockValIterator.nextIntVal(entries);
        int originSize = groupKeysList.size();
        for (int j = 0; j < group - 1; ++j) {
          for (int k = 0; k < originSize; ++k) {
            groupKeysList.add(groupKeysList.get(k));
          }
        }
        for (int j = 0; j < group; ++j) {
          for (int k = 0; k < originSize; ++k) {
            groupKeysList.set(j * originSize + k, (groupKeysList.get(j * originSize + k) << element) | entries[j]);
          }
        }
      }
      i++;
    }
    return groupKeysList.toArray(new Long[0]);
  }

  private long getGroupKey(int docId) {
    long ret = 0L;
    int i = 0;
    BlockSingleValIterator blockValIterator;
    for (final int element : _groupKeyBitSize) {
      blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i++];
      blockValIterator.skipTo(docId);
      ret = ret << element;
      ret |= blockValIterator.nextIntVal();
    }
    return ret;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in MAggregationFunctionGroupByOperator");
  }

  @Override
  public String getOperatorName() {
    return "MAggregationFunctionGroupByWithDictionaryOperator";
  }

  @Override
  public Map<String, Serializable> getAggregationGroupByResult() {
    _aggregateGroupedValue.clear();
    for (final long key : _tempAggregationResults.keySet()) {
      _aggregateGroupedValue.put(decodeGroupedKeyFromLong(key), _tempAggregationResults.get(key));
    }
    return _aggregateGroupedValue;
  }

  private String decodeGroupedKeyFromLong(long key) {
    int i = _groupKeyBitSize.length - 1;
    while (i >= 0) {
      final long number = key & (-1L >>> (64 - _groupKeyBitSize[i]));
      _stringArray[i] = _dictionaries[i].get((int) number).toString();
      key >>>= _groupKeyBitSize[i];
      i--;
    }

    final StringBuilder builder = new StringBuilder();
    for (int j = 0; j < (_stringArray.length - 1); j++) {
      builder.append(_stringArray[j]).append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString());
    }
    builder.append(_stringArray[_stringArray.length - 1]);
    return builder.toString();
  }
}
