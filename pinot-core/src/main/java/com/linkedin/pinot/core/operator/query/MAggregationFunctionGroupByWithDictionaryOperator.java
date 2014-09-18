package com.linkedin.pinot.core.operator.query;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.Serializable;
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
import com.linkedin.pinot.core.query.aggregation.groupby.BitHacks;
import com.linkedin.pinot.core.query.aggregation.groupby.GroupByConstants;


/**
 * This GroupBy Operator takes all the required dataSources. For groupBy columns,
 * it creates a long value as group key instead of a String.
 * This will make the algorithm performs better.
 * 
 * GetAggregationGroupByResult will return the results.
 * 
 * @author xiafu
 *
 */
public class MAggregationFunctionGroupByWithDictionaryOperator extends AggregationFunctionGroupByOperator {

  private Dictionary[] _dictionaries;
  private int[] _groupKeyBitSize;
  private final String[] _stringArray;

  private final Long2ObjectOpenHashMap<Serializable> _tempAggregationResults =
      new Long2ObjectOpenHashMap<Serializable>();

  public MAggregationFunctionGroupByWithDictionaryOperator(AggregationInfo aggregationInfo, GroupBy groupBy,
      Operator projectionOperator) {
    super(aggregationInfo, groupBy, projectionOperator);

    setGroupKeyOffset();
    _stringArray = new String[_groupKeyBitSize.length];
  }

  private void setGroupKeyOffset() {
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
    _groupKeyBitSize = new int[_dictionaries.length];
    int totalBitSet = 0;
    for (int i = 0; i < _dictionaries.length; i++) {
      _groupKeyBitSize[i] = BitHacks.findLogBase2(_dictionaries[i].size()) + 1;
      totalBitSet += _groupKeyBitSize[i];
    }
    if (totalBitSet > 64) {
      throw new IllegalArgumentException("Too many columns for an efficient group by");
    }
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
        long groupKey = getNextGroupKey();

        _tempAggregationResults.put(groupKey, _aggregationFunction.aggregate(_tempAggregationResults.get(groupKey),
            _aggregationFunctionBlockValIterators));
      }
    }
    return null;
  }

  @Override
  public Block nextBlock(BlockId BlockId) {
    throw new UnsupportedOperationException(
        "Method: nextBlock(BlockId BlockId) is Not Supported in MAggregationFunctionGroupByOperator");
  }

  @Override
  public Map<String, Serializable> getAggregationGroupByResult() {
    _aggregateGroupedValue.clear();
    for (long key : _tempAggregationResults.keySet()) {
      _aggregateGroupedValue.put(decodeGroupedKeyFromLong(_stringArray, _dictionaries, _groupKeyBitSize, key),
          _tempAggregationResults.get(key));
    }
    return _aggregateGroupedValue;
  }

  private long getNextGroupKey() {
    long ret = 0L;
    for (int i = 0; i < _groupKeyBitSize.length; ++i) {
      ret = ret << _groupKeyBitSize[i];
      ret |= _groupByBlockValIterators[i].nextDictVal();
    }
    return ret;
  }

  private String decodeGroupedKeyFromLong(String[] str, Dictionary[] dictionaries, int[] bitOffset, long key) {
    int i = bitOffset.length - 1;
    while (i >= 0) {
      long number = key & (-1L >>> (64 - bitOffset[i]));
      str[i] = dictionaries[i].getString((int) number);
      key >>>= bitOffset[i];

      i--;
    }

    StringBuilder builder = new StringBuilder();
    for (int j = 0; j < (str.length - 1); j++) {
      builder.append(str[j]).append(GroupByConstants.GroupByDelimiter.groupByMultiDelimeter.toString());
    }
    builder.append(str[str.length - 1]);
    return builder.toString();
  }
}
