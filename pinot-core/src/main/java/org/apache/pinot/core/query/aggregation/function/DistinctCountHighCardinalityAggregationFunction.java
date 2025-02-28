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
package org.apache.pinot.core.query.aggregation.function;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.query.NonScanBasedAggregationOperator;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Aggregation function to compute the count of distinct values for a high cardinality SV column.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountHighCardinalityAggregationFunction extends BaseDistinctAggregateAggregationFunction<Integer> {
  private static final int DEFAULT_INITIAL_CAPACITY = 10_000;

  private final boolean _sharedAcrossOperators;
  private final int _initialCapacity;

  // When the function is shared across operators without group-by, merge results into this share value set.
  private ConcurrentHashMap.KeySetView _sharedValueSet;

  public DistinctCountHighCardinalityAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled,
      boolean sharedAcrossOperators) {
    super(arguments.get(0), AggregationFunctionType.DISTINCTCOUNTHIGHCARDINALITY, nullHandlingEnabled);
    _sharedAcrossOperators = sharedAcrossOperators;
    if (arguments.size() > 1) {
      _initialCapacity = arguments.get(1).getLiteral().getIntValue();
    } else {
      _initialCapacity = 0;
    }
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (!_sharedAcrossOperators) {
      svAggregate(length, aggregationResultHolder, blockValSetMap);
      return;
    }

    BlockValSet blockValSet = blockValSetMap.get(_expression);
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      // For dictionary-encoded expression, store dictionary ids into the bitmap
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      BitSet dictIdBitSet = getDictIdBitSet(aggregationResultHolder, dictionary);
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          dictIdBitSet.set(dictIds[i]);
        }
      });
    } else {
      // For non-dictionary-encoded expression, directly add values into the shared value set
      synchronized (this) {
        if (_sharedValueSet == null) {
          int initialCapacity = _initialCapacity > 0 ? _initialCapacity : DEFAULT_INITIAL_CAPACITY;
          _sharedValueSet = ConcurrentHashMap.newKeySet(initialCapacity);
        }
      }
      addToSharedValueSet(length, blockValSet);
    }
  }

  private static BitSet getDictIdBitSet(AggregationResultHolder aggregationResultHolder, Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._bitSet;
  }

  private void addToSharedValueSet(int length, BlockValSet blockValSet) {
    ConcurrentHashMap.KeySetView valueSet = _sharedValueSet;
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            valueSet.add(intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            valueSet.add(longValues[i]);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            valueSet.add(floatValues[i]);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            valueSet.add(doubleValues[i]);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          //noinspection ManualArrayToCollectionCopy
          for (int i = from; i < to; i++) {
            valueSet.add(stringValues[i]);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            valueSet.add(new ByteArray(bytesValues[i]));
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    svAggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    svAggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public Set extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    if (!_sharedAcrossOperators) {
      return super.extractAggregationResult(aggregationResultHolder);
    }

    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      // Result could be null in 2 scenarios:
      // - Values directly added to the shared value set: return the shared value set
      // - No value ever added: use empty IntOpenHashSet as a placeholder for empty result
      return _sharedValueSet != null ? _sharedValueSet : new IntOpenHashSet();
    }
    DictIdsWrapper dictIdsWrapper = (DictIdsWrapper) result;
    Dictionary dictionary = dictIdsWrapper._dictionary;
    BitSet bitSet = dictIdsWrapper._bitSet;
    synchronized (this) {
      if (_sharedValueSet == null) {
        int initialCapacity = _initialCapacity > 0 ? _initialCapacity : bitSet.cardinality();
        _sharedValueSet = ConcurrentHashMap.newKeySet(initialCapacity);
      }
    }
    ConcurrentHashMap.KeySetView valueSet = _sharedValueSet;
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      valueSet.add(dictionary.getInternal(i));
    }
    return valueSet;
  }

  /**
   * Extracts the value set from the dictionary.
   */
  public Set extractAggregationResult(Dictionary dictionary) {
    if (!_sharedAcrossOperators) {
      return NonScanBasedAggregationOperator.getDistinctValueSet(dictionary);
    }

    int length = dictionary.length();
    synchronized (this) {
      if (_sharedValueSet == null) {
        int initialCapacity = _initialCapacity > 0 ? _initialCapacity : length;
        _sharedValueSet = ConcurrentHashMap.newKeySet(initialCapacity);
      }
    }
    ConcurrentHashMap.KeySetView valueSet = _sharedValueSet;
    for (int i = 0; i < length; i++) {
      valueSet.add(dictionary.getInternal(i));
    }
    return valueSet;
  }

  @Override
  public Set merge(Set intermediateResult1, Set intermediateResult2) {
    return _sharedValueSet != null ? _sharedValueSet : super.merge(intermediateResult1, intermediateResult2);
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(Set intermediateResult) {
    return intermediateResult.size();
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return finalResult1 + finalResult2;
  }

  // Different from the BaseDistinctAggregateAggregationFunction.DictIdsWrapper, here we use a pre-allocated BitSet
  // instead of RoaringBitmap for better performance on high cardinality distinct count.
  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final BitSet _bitSet;

    DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _bitSet = new BitSet(dictionary.length());
    }
  }
}
