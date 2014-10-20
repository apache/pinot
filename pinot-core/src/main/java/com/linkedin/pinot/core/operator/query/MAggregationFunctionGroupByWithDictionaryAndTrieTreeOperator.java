package com.linkedin.pinot.core.operator.query;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.Serializable;
import java.util.Map;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.Operator;
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

  private final BlockValSet[] _blockValSets;
  private final TrieNode _rootNode;
  private int[] _groupKeys;

  public MAggregationFunctionGroupByWithDictionaryAndTrieTreeOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator) {
    super(aggregationInfo, groupBy, projectionOperator);
    _blockValSets = new BlockValSet[_groupBy.getColumnsSize()];
    _groupKeys = new int[_groupBy.getColumnsSize()];
    for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
      if (_projectionOperator instanceof UReplicatedProjectionOperator) {
        _blockValSets[i] =
            ((UReplicatedProjectionOperator) _projectionOperator).getProjectionOperator()
                .getDataSource(_groupBy.getColumns().get(i)).nextBlock(new BlockId(0)).getBlockValueSet();
      } else if (_projectionOperator instanceof MProjectionOperator) {
        _blockValSets[i] =
            ((MProjectionOperator) _projectionOperator).getDataSource(_groupBy.getColumns().get(i))
                .nextBlock(new BlockId(0)).getBlockValueSet();
      }
    }
    _rootNode = new TrieNode();
  }

  @Override
  public Block nextBlock() {
    ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
    if (block != null) {
      for (int i = 0; i < _groupBy.getColumnsSize(); ++i) {
        _groupByBlockValIterators[i] = block.getBlockValueSetIterator(_groupBy.getColumns().get(i));
        // block.getBlock(_groupBy.getColumns().get(i)).getBlockValueSet().iterator();
      }
      for (int i = 0; i < _aggregationColumns.length; ++i) {
        _aggregationFunctionBlockValIterators[i] = block.getBlockValueSetIterator(_aggregationColumns[i]);
        // block.getBlock(_aggregationColumns[i]).getBlockValueSet().iterator();
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
    // traverseTrieTree(_rootNode, new ArrayList<Integer>());
    traverseTrieTree(_rootNode, 0);
    return _aggregateGroupedValue;
  }

  private void traverseTrieTree(TrieNode rootNode, int level) {
    if (rootNode.getNextGroupedColumnValues() != null) {
      for (int key : rootNode.getNextGroupedColumnValues().keySet()) {
        _groupKeys[level] = key;
        traverseTrieTree(rootNode.getNextGroupedColumnValues().get(key), level + 1);
      }
    } else {
      _aggregateGroupedValue.put(getGroupedKey(), rootNode.getAggregationResult());
    }
  }

  private String getGroupedKey() {
    StringBuilder sb = new StringBuilder();
    sb.append(_blockValSets[0].getStringValueAt(_groupKeys[0]));
    for (int i = 1; i < _groupKeys.length; ++i) {
      sb.append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString()
          + _blockValSets[i].getStringValueAt(_groupKeys[i]));
    }
    return sb.toString();
  }

}
