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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

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
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.query.utils.TrieNode;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;


/**
 * This GroupBy Operator takes all the required dataSources. For groupBy columns,
 * it creates a trie tree for all the groups. Each groupBy column value is a node
 * in the Trie. Leaf node will store the aggregation results.
 * GetAggregationGroupByResult will return the results.
 *
 * and do aggregation and groupBy.
 *
 *
 */
public class MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator extends AggregationFunctionGroupByOperator {

  private final Dictionary[] _dictionaries;
  private final BlockValIterator[] _groupByBlockValIterators;

  private final TrieNode _rootNode;
  private final int[] _groupKeys;

  public MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator, boolean hasDictionary) {
    super(aggregationInfo, groupBy, projectionOperator, hasDictionary);
    _groupKeys = new int[_groupBy.getColumnsSize()];
    _dictionaries = new Dictionary[_groupBy.getColumnsSize()];

    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      _dictionaries[i] = _groupByBlocks[i].getMetadata().getDictionary();
    }
    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];
    _rootNode = new TrieNode();
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

        TrieNode currentNode = _rootNode;
        for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
          if (currentNode.getNextGroupedColumnValues() == null) {
            currentNode.setNextGroupedColumnValues(new Int2ObjectOpenHashMap<TrieNode>());
          }
          BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
          blockValIterator.skipTo(docId);
          final int groupKey = blockValIterator.nextIntVal();

          if (!currentNode.getNextGroupedColumnValues().containsKey(groupKey)) {
            currentNode.getNextGroupedColumnValues().put(groupKey, new TrieNode());
          }
          currentNode = currentNode.getNextGroupedColumnValues().get(groupKey);
        }
        currentNode.setAggregationResult(_aggregationFunction.aggregate(currentNode.getAggregationResult(), docId,
            _aggregationFunctionBlocks));
      } else {
        List<TrieNode> currentNodesList = new ArrayList<TrieNode>();
        currentNodesList.add(_rootNode);
        for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
          if (_isSingleValueGroupByColumn[i]) {
            for (int j = 0; j < currentNodesList.size(); ++j) {
              TrieNode currentNode = currentNodesList.get(j);
              if (currentNode.getNextGroupedColumnValues() == null) {
                currentNode.setNextGroupedColumnValues(new Int2ObjectOpenHashMap<TrieNode>());
              }
              BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
              blockValIterator.skipTo(docId);
              final int groupKey = blockValIterator.nextIntVal();
              if (!currentNode.getNextGroupedColumnValues().containsKey(groupKey)) {
                currentNode.getNextGroupedColumnValues().put(groupKey, new TrieNode());
              }
              currentNode = currentNode.getNextGroupedColumnValues().get(groupKey);
              currentNodesList.set(j, currentNode);
            }
          } else {
            BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[i];
            blockValIterator.skipTo(docId);
            final int maxValue = _groupByBlocks[i].getMetadata().getMaxNumberOfMultiValues();
            final int[] entries = new int[maxValue];
            int group = blockValIterator.nextIntVal(entries);
            if (group == 0) {
              group = 1;
              entries[0] = -1;
            }
            int originSize = currentNodesList.size();
            for (int k = 0; k < group - 1; ++k) {
              for (int l = 0; l < originSize; ++l) {
                currentNodesList.add(currentNodesList.get(l));
              }
            }
            for (int k = 0; k < currentNodesList.size(); ++k) {
              TrieNode currentNode = currentNodesList.get(k);
              if (currentNode.getNextGroupedColumnValues() == null) {
                currentNode.setNextGroupedColumnValues(new Int2ObjectOpenHashMap<TrieNode>());
              }
              if (!currentNode.getNextGroupedColumnValues().containsKey(entries[k / originSize])) {
                currentNode.getNextGroupedColumnValues().put(entries[k / originSize], new TrieNode());
              }
              currentNode = currentNode.getNextGroupedColumnValues().get(entries[k / originSize]);
              currentNodesList.set(k, currentNode);
            }
          }
          for (TrieNode currentNode : currentNodesList) {
            currentNode.setAggregationResult(_aggregationFunction.aggregate(currentNode.getAggregationResult(), docId,
                _aggregationFunctionBlocks));
          }
        }
      }
    }

    return null;
  }

  @Override
  public Block getNextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator");
  }

  @Override
  public String getOperatorName() {
    return "MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator";
  }

  @Override
  public Map<String, Serializable> getAggregationGroupByResult() {
    traverseTrieTree(_rootNode, 0);
    return _aggregateGroupedValue;
  }

  private void traverseTrieTree(TrieNode rootNode, int level) {
    if (rootNode.getNextGroupedColumnValues() != null) {
      for (final int key : rootNode.getNextGroupedColumnValues().keySet()) {
        _groupKeys[level] = key;
        traverseTrieTree(rootNode.getNextGroupedColumnValues().get(key), level + 1);
      }
    } else {
      _aggregateGroupedValue.put(getGroupedKey(), rootNode.getAggregationResult());
    }
  }

  private String getGroupedKey() {
    final StringBuilder sb = new StringBuilder();
    sb.append(_dictionaries[0].get(_groupKeys[0]).toString());
    for (int i = 1; i < _groupKeys.length; ++i) {
      sb.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString()
          + _dictionaries[i].get(_groupKeys[i]).toString());
    }
    return sb.toString();
  }
}
