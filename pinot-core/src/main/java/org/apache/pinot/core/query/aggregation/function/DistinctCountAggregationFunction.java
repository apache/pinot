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
import java.util.Arrays;
import java.util.Map;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


public class DistinctCountAggregationFunction extends BaseSingleInputAggregationFunction<IntOpenHashSet, Integer> {

  public DistinctCountAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNT;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    IntOpenHashSet valueSet = getValueSet(aggregationResultHolder);
    DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(stringValues[i].hashCode());
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          valueSet.add(Arrays.hashCode(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i]).add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i]).add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i]).add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i]).add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i]).add(stringValues[i].hashCode());
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          getValueSet(groupByResultHolder, groupKeyArray[i]).add(Arrays.hashCode(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    DataType valueType = blockValSet.getValueType();
    switch (valueType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i].hashCode());
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], Arrays.hashCode(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
  }

  @Override
  public IntOpenHashSet extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new IntOpenHashSet();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (IntOpenHashSet) result;
    }
  }

  @Override
  public IntOpenHashSet extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new IntOpenHashSet();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (IntOpenHashSet) result;
    }
  }

  @Override
  public IntOpenHashSet merge(IntOpenHashSet intermediateResult1, IntOpenHashSet intermediateResult2) {
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public boolean isIntermediateResultComparable() {
    return false;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(IntOpenHashSet intermediateResult) {
    return intermediateResult.size();
  }

  /**
   * Returns the dictionary id bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value set from the result holder or creates a new one if it does not exist.
   */
  protected static IntOpenHashSet getValueSet(AggregationResultHolder aggregationResultHolder) {
    IntOpenHashSet valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      aggregationResultHolder.setValue(valueSet);
    }
    return valueSet;
  }

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getDictIdBitmap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdBitmap;
  }

  /**
   * Returns the value set for the given group key or creates a new one if it does not exist.
   */
  protected static IntOpenHashSet getValueSet(GroupByResultHolder groupByResultHolder, int groupKey) {
    IntOpenHashSet valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = new IntOpenHashSet();
      groupByResultHolder.setValueForKey(groupKey, valueSet);
    }
    return valueSet;
  }

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  private static void setDictIdForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys,
      Dictionary dictionary, int dictId) {
    for (int groupKey : groupKeys) {
      getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictId);
    }
  }

  /**
   * Helper method to set value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      getValueSet(groupByResultHolder, groupKey).add(value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
   * expression.
   */
  private static IntOpenHashSet convertToValueSet(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    IntOpenHashSet valueSet = new IntOpenHashSet(dictIdBitmap.getCardinality());
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType valueType = dictionary.getValueType();
    switch (valueType) {
      case INT:
        while (iterator.hasNext()) {
          valueSet.add(dictionary.getIntValue(iterator.next()));
        }
        break;
      case LONG:
        while (iterator.hasNext()) {
          valueSet.add(Long.hashCode(dictionary.getLongValue(iterator.next())));
        }
        break;
      case FLOAT:
        while (iterator.hasNext()) {
          valueSet.add(Float.hashCode(dictionary.getFloatValue(iterator.next())));
        }
        break;
      case DOUBLE:
        while (iterator.hasNext()) {
          valueSet.add(Double.hashCode(dictionary.getDoubleValue(iterator.next())));
        }
        break;
      case STRING:
        while (iterator.hasNext()) {
          valueSet.add(dictionary.getStringValue(iterator.next()).hashCode());
        }
        break;
      case BYTES:
        while (iterator.hasNext()) {
          valueSet.add(Arrays.hashCode(dictionary.getBytesValue(iterator.next())));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
    return valueSet;
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new RoaringBitmap();
    }
  }
}
