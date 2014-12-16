package com.linkedin.pinot.core.operator.query;

import java.util.ArrayList;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.common.request.GroupBy;
import com.linkedin.pinot.core.block.query.ProjectionBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.DocIdSetBlock;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;
import com.linkedin.pinot.core.segment.index.readers.DictionaryReader;


/**
 * The most generic GroupBy Operator which will take all the required dataSources
 * and do aggregation and groupBy.
 * 
 * @author xiafu
 *
 */
public class MAggregationFunctionGroupByOperator extends AggregationFunctionGroupByOperator {

  private final BlockValIterator[] _groupByBlockValIterators;

  public MAggregationFunctionGroupByOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator) {
    super(aggregationInfo, groupBy, projectionOperator);
    _groupByBlockValIterators = new BlockValIterator[_groupBy.getColumnsSize()];

  }

  @Override
  public Block nextBlock() {
    ProjectionBlock block = (ProjectionBlock) _projectionOperator.nextBlock();
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
        String groupKey = getGroupKey(docId);
        _aggregateGroupedValue.put(groupKey,
            _aggregationFunction.aggregate(_aggregateGroupedValue.get(groupKey), docId, _aggregationFunctionBlocks));
      } else {
        String[] groupKeys = getGroupKeys(docId);
        for (String groupKey : groupKeys) {
          _aggregateGroupedValue.put(groupKey,
              _aggregationFunction.aggregate(_aggregateGroupedValue.get(groupKey), docId, _aggregationFunctionBlocks));
        }
      }
    }
    return null;
  }

  private String getGroupKey(int docId) {

    BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[0];
    DictionaryReader dictionaryReader = _groupByBlocks[0].getMetadata().getDictionary();
    blockValIterator.skipTo(docId);
    String groupKey = dictionaryReader.get(blockValIterator.nextIntVal()).toString();
    for (int i = 1; i < _groupByBlockValIterators.length; ++i) {
      blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
      blockValIterator.skipTo(docId);
      dictionaryReader = _groupByBlocks[i].getMetadata().getDictionary();
      groupKey +=
          (GroupByConstants.GroupByDelimiter.groupByMultiDelimeter + (dictionaryReader.get(blockValIterator
              .nextIntVal()).toString()));
    }
    return groupKey;
  }

  private String[] getGroupKeys(int docId) {
    ArrayList<String> groupKeyList = new ArrayList<String>();
    DictionaryReader dictionaryReader = _groupByBlocks[0].getMetadata().getDictionary();
    if (_groupByBlocks[0].getMetadata().isSingleValue()) {
      BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[0];
      blockValIterator.skipTo(docId);
      groupKeyList.add(dictionaryReader.get(blockValIterator.nextIntVal()).toString());
    } else {
      BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[0];
      blockValIterator.skipTo(docId);
      final int maxValue = _groupByBlocks[0].getMetadata().maxNumberOfMultiValues();
      final int[] entries = new int[maxValue];
      int groups = blockValIterator.nextIntVal(entries);
      for (int i = 0; i < groups; ++i) {
        groupKeyList.add((dictionaryReader.get(entries[i])).toString());
      }
    }

    for (int i = 1; i < _groupByBlockValIterators.length; ++i) {
      dictionaryReader = _groupByBlocks[i].getMetadata().getDictionary();
      if (_groupByBlocks[i].getMetadata().isSingleValue()) {
        BlockSingleValIterator blockValIterator = (BlockSingleValIterator) _groupByBlockValIterators[i];
        blockValIterator.skipTo(docId);
        for (int j = 0; j < groupKeyList.size(); ++j) {
          groupKeyList.set(
              j,
              groupKeyList.get(j)
                  + (GroupByConstants.GroupByDelimiter.groupByMultiDelimeter + (dictionaryReader.get(blockValIterator
                      .nextIntVal()).toString())));
        }
      } else {
        // Multivalue
        BlockMultiValIterator blockValIterator = (BlockMultiValIterator) _groupByBlockValIterators[i];
        blockValIterator.skipTo(docId);
        final int maxValue = _groupByBlocks[i].getMetadata().maxNumberOfMultiValues();
        final int[] entries = new int[maxValue];
        int groups = blockValIterator.nextIntVal(entries);
        int currentGroupListSize = groupKeyList.size();

        for (int j = 1; j < groups; ++j) {
          for (int k = 0; k < currentGroupListSize; ++k) {
            groupKeyList.add(groupKeyList.get(k));
          }
        }

        for (int j = 0; j < groupKeyList.size(); ++j) {
          groupKeyList.set(j, groupKeyList.get(j) + GroupByConstants.GroupByDelimiter.groupByMultiDelimeter
              + (dictionaryReader.get(entries[j / currentGroupListSize])).toString());

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
