package com.linkedin.pinot.core.operator.query;

import java.util.ArrayList;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;


/**
 * The most generic GroupBy Operator which will take all the required dataSources
 * and do aggregation and groupBy.
 * 
 * @author xiafu
 *
 */
public class MAggregationFunctionGroupByOperator extends AggregationFunctionGroupByOperator {

  private final BlockValIterator[] _aggregationFunctionBlockValIterators;
  private final BlockValIterator[] _groupByBlockValIterators;

  public MAggregationFunctionGroupByOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator) {
    super(aggregationInfo, groupBy, projectionOperator);
    _aggregationFunctionBlockValIterators = new BlockValIterator[_aggregationColumns.length];
    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];

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
    }
    while (_groupByBlockValIterators[0].hasNext()) {
      if (_isGroupByColumnsContainMultiValueColumn) {
        String groupKey = getGroupKey();
        _aggregateGroupedValue
            .put(groupKey,
                _aggregationFunction.aggregate(_aggregateGroupedValue.get(groupKey),
                    _aggregationFunctionBlockValIterators));
      } else {
        String[] groupKeys = getGroupKeys();
        for (String groupKey : groupKeys) {
          _aggregateGroupedValue
              .put(groupKey,
                  _aggregationFunction.aggregate(_aggregateGroupedValue.get(groupKey),
                      _aggregationFunctionBlockValIterators));
        }
      }
    }
    return null;
  }

  private String getGroupKey() {
    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[0];

    String groupKey = new String(blockValIterator.nextBytesVal());
    for (int i = 1; i < _groupByBlockValIterators.length; ++i) {
      blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
      groupKey +=
          (GroupByConstants.GroupByDelimiter.groupByMultiDelimeter + new String(blockValIterator.nextBytesVal()));
    }
    return groupKey;
  }

  private String[] getGroupKeys() {
    ArrayList<String> groupKeyList = new ArrayList<String>();
    if (_groupByBlocks[0].getMetadata().isSingleValue()) {
      BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[0];
      groupKeyList.add(_groupByBlocks[0].getMetadata().getDictionary().get(blockValIterator.nextIntVal()).toString());
    } else {
      BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[0];
      final int maxValue =
          _groupByBlocks[0].getMetadata().maxNumberOfMultiValues();
      final int[] entries = new int[maxValue];
      int groups = blockValIterator.nextIntVal(entries);
      for (int i = 0; i < groups; ++i) {
        groupKeyList.add((_groupByBlocks[0].getMetadata().getDictionary().get(entries[i])).toString());
      }
    }

    for (int i = 1; i < _groupByBlockValIterators.length; ++i) {
      if (_groupByBlocks[i].getMetadata().isSingleValue()) {
        BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
        for (int j = 0; j < groupKeyList.size(); ++j) {
          groupKeyList
              .set(
                  j,
                  groupKeyList.get(j)
                      + (GroupByConstants.GroupByDelimiter.groupByMultiDelimeter + new String(blockValIterator
                          .nextBytesVal())));

          ;
        }
      } else {
        // Multivalue
        BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[i];
        final int maxValue =
            _groupByBlocks[i].getMetadata().maxNumberOfMultiValues();
        final int[] entries = new int[maxValue];
        int groups = blockValIterator.nextIntVal(entries);
        int currentGroupListSize = groupKeyList.size();
        for (int j = 1; j < groups; ++j) {
          for (int k = 0; k < currentGroupListSize; ++k) {
            groupKeyList.add(groupKeyList.get(k) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter +
                (_groupByBlocks[i].getMetadata().getDictionary().get(entries[j])).toString());
          }
        }
        String newGroupKeyEntry = "";
        if (groups > 0) {
          newGroupKeyEntry = (_groupByBlocks[i].getMetadata().getDictionary().get(entries[0])).toString();
        }
        for (int k = 0; k < currentGroupListSize; ++k) {
          groupKeyList.add(groupKeyList.get(k) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
              + newGroupKeyEntry);
        }
      }
    }
    return groupKeyList.toArray(new String[0]);
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in MAggregationFunctionGroupByOperator");
  }

}
