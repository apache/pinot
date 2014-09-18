package com.linkedin.pinot.core.operator.query;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.operator.MProjectionOperator;
import com.linkedin.pinot.core.operator.UReplicatedProjectionOperator;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.query.utils.TrieNode;


/**
 * This GroupBy Operator takes all the required dataSources. For groupBy columns,
 * it creates a trie tree for all the groups. Each groupBy column value is a node
 * in the Trie. Leaf node will store the aggregation results.
 * GetAggregationGroupByResult will return the results.
 * 
 * and do aggregation and groupBy.
 * 
 * @author xiafu
 *
 */
public class MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator extends AggregationFunctionGroupByOperator {

  private final Dictionary[] _dictionaries;
  private final TrieNode _rootNode;
  private final Long2ObjectOpenHashMap<Serializable> _tempAggregationResults =
      new Long2ObjectOpenHashMap<Serializable>();

  public MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator) {
    super(aggregationInfo, groupBy, projectionOperator);

    _dictionaries = new Dictionary[_groupBy.getColumnsSize()];
    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      if (_projectionOperator instanceof UReplicatedProjectionOperator) {
        _dictionaries[i] =
            ((UReplicatedProjectionOperator) _projectionOperator).getProjectionOperator().getDictionary(
                _groupBy.getColumns().get(i));
      } else if (_projectionOperator instanceof MProjectionOperator) {
        _dictionaries[i] = ((MProjectionOperator) _projectionOperator).getDictionary(_groupBy.getColumns().get(i));
      }
    }
    _rootNode = new TrieNode();
  }

  @Override
  public Block nextBlock() {
    ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
    if (block != null) {
      for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
        _groupByBlockValIterators[i] = block.getBlock(_groupBy.getColumns().get(i)).getBlockValueSet().iterator();
      }
      for (int i = 0; i < _aggregationColumns.length; ++i) {
        _aggregationFunctionBlockValIterators[i] = block.getBlock(_aggregationColumns[i]).getBlockValueSet().iterator();
      }

      while (_groupByBlockValIterators[0].hasNext()) {

        TrieNode currentNode = _rootNode;
        for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
          if (currentNode.getNextGroupedColumnValues() == null) {
            currentNode.setNextGroupedColumnValues(new Int2ObjectOpenHashMap<TrieNode>());
          }
          int groupKey = _groupByBlockValIterators[i].nextDictVal();

          if (!currentNode.getNextGroupedColumnValues().containsKey(groupKey)) {
            currentNode.getNextGroupedColumnValues().put(groupKey, new TrieNode());
          }
          currentNode = currentNode.getNextGroupedColumnValues().get(groupKey);
        }
        currentNode.setAggregationResult(_aggregationFunction.aggregate(currentNode.getAggregationResult(),
            _aggregationFunctionBlockValIterators));
      }
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator");
  }

  @Override
  public Map<String, Serializable> getAggregationGroupByResult() {
    traverseTrieTree(_rootNode, new ArrayList<Integer>(), _dictionaries);
    return _aggregateGroupedValue;
  }

  private void traverseTrieTree(TrieNode rootNode, List<Integer> groupedKey, Dictionary[] dictionaries) {
    if (rootNode.getNextGroupedColumnValues() != null) {
      for (int key : rootNode.getNextGroupedColumnValues().keySet()) {
        groupedKey.add(key);
        traverseTrieTree(rootNode.getNextGroupedColumnValues().get(key), groupedKey, dictionaries);
        groupedKey.remove(groupedKey.size() - 1);
      }
    } else {
      _aggregateGroupedValue.put(getGroupedKey(groupedKey, dictionaries), rootNode.getAggregationResult());
    }
  }

  private String getGroupedKey(List<Integer> groupedKey, Dictionary[] dictionaries) {
    StringBuilder sb = new StringBuilder();
    sb.append(dictionaries[0].getString(groupedKey.get(0)));
    for (int i = 1; i < groupedKey.size(); ++i) {
      sb.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString()
          + dictionaries[i].getString(groupedKey.get(i)));
    }
    return sb.toString();
  }
}
