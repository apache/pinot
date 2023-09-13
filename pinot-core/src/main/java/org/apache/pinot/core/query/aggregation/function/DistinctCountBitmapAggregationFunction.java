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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DistinctCountBitmapAggregationFunction} calculates the number of distinct values for a given single-value
 * expression using RoaringBitmap. The bitmap stores the actual values for {@code INT} expression, or hash code of the
 * values for other data types (values with the same hash code will only be counted once).
 */
public class DistinctCountBitmapAggregationFunction extends BaseSingleInputAggregationFunction<RoaringBitmap, Integer> {

  public DistinctCountBitmapAggregationFunction(List<ExpressionContext> arguments) {
    this(verifySingleArgument(arguments, "DISTINCT_COUNT_BITMAP"));
  }

  protected DistinctCountBitmapAggregationFunction(ExpressionContext expression) {
    super(expression);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTBITMAP;
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

    // Treat BYTES value as serialized RoaringBitmap
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      RoaringBitmap valueBitmap = aggregationResultHolder.getResult();
      if (valueBitmap != null) {
        for (int i = 0; i < length; i++) {
          valueBitmap.or(RoaringBitmapUtils.deserialize(bytesValues[i]));
        }
      } else {
        valueBitmap = RoaringBitmapUtils.deserialize(bytesValues[0]);
        aggregationResultHolder.setValue(valueBitmap);
        for (int i = 1; i < length; i++) {
          valueBitmap.or(RoaringBitmapUtils.deserialize(bytesValues[i]));
        }
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    RoaringBitmap valueBitmap = getValueBitmap(aggregationResultHolder);
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        valueBitmap.addN(intValues, 0, length);
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          valueBitmap.add(stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized RoaringBitmap
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        RoaringBitmap value = RoaringBitmapUtils.deserialize(bytesValues[i]);
        int groupKey = groupKeyArray[i];
        RoaringBitmap valueBitmap = groupByResultHolder.getResult(groupKey);
        if (valueBitmap != null) {
          valueBitmap.or(value);
        } else {
          groupByResultHolder.setValueForKey(groupKey, value);
        }
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Long.hashCode(longValues[i]));
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Float.hashCode(floatValues[i]));
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(Double.hashCode(doubleValues[i]));
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          getValueBitmap(groupByResultHolder, groupKeyArray[i]).add(stringValues[i].hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // Treat BYTES value as serialized RoaringBitmap
    DataType storedType = blockValSet.getValueType().getStoredType();
    if (storedType == DataType.BYTES) {
      byte[][] bytesValues = blockValSet.getBytesValuesSV();
      for (int i = 0; i < length; i++) {
        RoaringBitmap value = RoaringBitmapUtils.deserialize(bytesValues[i]);
        for (int groupKey : groupKeysArray[i]) {
          RoaringBitmap bitmap = groupByResultHolder.getResult(groupKey);
          if (bitmap != null) {
            bitmap.or(value);
          } else {
            // Clone a bitmap for the group
            groupByResultHolder.setValueForKey(groupKey, value.clone());
          }
        }
      }
      return;
    }

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the bitmap
    switch (storedType) {
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
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
  }

  @Override
  public RoaringBitmap extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return new RoaringBitmap();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueBitmap((DictIdsWrapper) result);
    } else {
      // For serialized RoaringBitmap and non-dictionary-encoded expression, directly return the value bitmap
      return (RoaringBitmap) result;
    }
  }

  @Override
  public RoaringBitmap extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return new RoaringBitmap();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to hash code of the values
      return convertToValueBitmap((DictIdsWrapper) result);
    } else {
      // For serialized RoaringBitmap and non-dictionary-encoded expression, directly return the value bitmap
      return (RoaringBitmap) result;
    }
  }

  @Override
  public RoaringBitmap merge(RoaringBitmap intermediateResult1, RoaringBitmap intermediateResult2) {
    intermediateResult1.or(intermediateResult2);
    return intermediateResult1;
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
  public Integer extractFinalResult(RoaringBitmap intermediateResult) {
    return intermediateResult.getCardinality();
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
   * Returns the value bitmap from the result holder or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getValueBitmap(AggregationResultHolder aggregationResultHolder) {
    RoaringBitmap bitmap = aggregationResultHolder.getResult();
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      aggregationResultHolder.setValue(bitmap);
    }
    return bitmap;
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
   * Returns the value bitmap for the given group key or creates a new one if it does not exist.
   */
  protected static RoaringBitmap getValueBitmap(GroupByResultHolder groupByResultHolder, int groupKey) {
    RoaringBitmap bitmap = groupByResultHolder.getResult(groupKey);
    if (bitmap == null) {
      bitmap = new RoaringBitmap();
      groupByResultHolder.setValueForKey(groupKey, bitmap);
    }
    return bitmap;
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
  private void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      getValueBitmap(groupByResultHolder, groupKey).add(value);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
   * expression.
   */
  private static RoaringBitmap convertToValueBitmap(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    RoaringBitmap valueBitmap = new RoaringBitmap();
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getIntValue(iterator.next()));
        }
        break;
      case LONG:
        while (iterator.hasNext()) {
          valueBitmap.add(Long.hashCode(dictionary.getLongValue(iterator.next())));
        }
        break;
      case FLOAT:
        while (iterator.hasNext()) {
          valueBitmap.add(Float.hashCode(dictionary.getFloatValue(iterator.next())));
        }
        break;
      case DOUBLE:
        while (iterator.hasNext()) {
          valueBitmap.add(Double.hashCode(dictionary.getDoubleValue(iterator.next())));
        }
        break;
      case STRING:
        while (iterator.hasNext()) {
          valueBitmap.add(dictionary.getStringValue(iterator.next()).hashCode());
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_BITMAP aggregation function: " + storedType);
    }
    return valueBitmap;
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
