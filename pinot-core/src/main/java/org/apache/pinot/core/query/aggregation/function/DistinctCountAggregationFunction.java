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

import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.RoaringBitmap;


@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountAggregationFunction extends BaseSingleInputAggregationFunction<Object, Integer> {

  private static final Object EMPTY_VALUE = new Object();

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

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    Set valueSet = getValueSet(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        IntOpenHashSet intSet = (IntOpenHashSet) valueSet;
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          intSet.add(intValues[i]);
        }
        break;
      case LONG:
        LongOpenHashSet longSet = (LongOpenHashSet) valueSet;
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          longSet.add(longValues[i]);
        }
        break;
      case FLOAT:
        FloatOpenHashSet floatSet = (FloatOpenHashSet) valueSet;
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          floatSet.add(floatValues[i]);
        }
        break;
      case DOUBLE:
        DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) valueSet;
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          doubleSet.add(doubleValues[i]);
        }
        break;
      case STRING:
        ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) valueSet;
        String[] stringValues = blockValSet.getStringValuesSV();
        //noinspection ManualArrayToCollectionCopy
        for (int i = 0; i < length; i++) {
          stringSet.add(stringValues[i]);
        }
        break;
      case BYTES:
        ObjectOpenHashSet<ByteArray> bytesSet = (ObjectOpenHashSet<ByteArray>) valueSet;
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          bytesSet.add(new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          ((IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT)).add(intValues[i]);
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          ((LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG)).add(longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          ((FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT)).add(floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          ((DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE))
              .add(doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          ((ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING))
              .add(stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          ((ObjectOpenHashSet<ByteArray>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.BYTES))
              .add(new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + storedType);
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

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
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
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        for (int i = 0; i < length; i++) {
          setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], new ByteArray(bytesValues[i]));
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + storedType);
    }
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return EMPTY_VALUE;
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return ((DictIdsWrapper) result)._dictIdBitmap;
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return result;
    }
  }

  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return EMPTY_VALUE;
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return ((DictIdsWrapper) result)._dictIdBitmap;
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return result;
    }
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    if (intermediateResult1 == EMPTY_VALUE) {
      return intermediateResult2;
    }
    if (intermediateResult2 == EMPTY_VALUE) {
      return intermediateResult1;
    }
    if (intermediateResult1 instanceof RoaringBitmap && intermediateResult2 instanceof RoaringBitmap) {
     return merge((RoaringBitmap) intermediateResult1, (RoaringBitmap) intermediateResult2);
    } else if (intermediateResult1 instanceof Set && intermediateResult2 instanceof Set) {
      return merge((Set) intermediateResult1, (Set) intermediateResult2);
    }
    throw new IllegalStateException("unexpected intermediate results of types "
        + intermediateResult1.getClass() + intermediateResult2.getClass());
  }

  private Set merge(Set intermediateResult1, Set intermediateResult2) {
    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  private RoaringBitmap merge(RoaringBitmap intermediateResult1, RoaringBitmap intermediateResult2) {
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
  public Integer extractFinalResult(Object intermediateResult) {
    if (intermediateResult instanceof RoaringBitmap) {
      return ((RoaringBitmap) intermediateResult).getCardinality();
    } else if (intermediateResult instanceof Set) {
      return ((Set) intermediateResult).size();
    } else if (intermediateResult == EMPTY_VALUE) {
      return 0;
    }
    throw new IllegalStateException("unexpected intermediate result of type " + intermediateResult.getClass());
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
  protected static Set getValueSet(AggregationResultHolder aggregationResultHolder, DataType valueType) {
    Set valueSet = aggregationResultHolder.getResult();
    if (valueSet == null) {
      valueSet = getValueSet(valueType);
      aggregationResultHolder.setValue(valueSet);
    }
    return valueSet;
  }

  /**
   * Helper method to create a value set for the given value type.
   */
  private static Set getValueSet(DataType valueType) {
    switch (valueType) {
      case INT:
        return new IntOpenHashSet();
      case LONG:
        return new LongOpenHashSet();
      case FLOAT:
        return new FloatOpenHashSet();
      case DOUBLE:
        return new DoubleOpenHashSet();
      case STRING:
      case BYTES:
        return new ObjectOpenHashSet();
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_COUNT aggregation function: " + valueType);
    }
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
  protected static Set getValueSet(GroupByResultHolder groupByResultHolder, int groupKey, DataType valueType) {
    Set valueSet = groupByResultHolder.getResult(groupKey);
    if (valueSet == null) {
      valueSet = getValueSet(valueType);
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
   * Helper method to set INT value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, int value) {
    for (int groupKey : groupKeys) {
      ((IntOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.INT)).add(value);
    }
  }

  /**
   * Helper method to set LONG value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, long value) {
    for (int groupKey : groupKeys) {
      ((LongOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.LONG)).add(value);
    }
  }

  /**
   * Helper method to set FLOAT value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, float value) {
    for (int groupKey : groupKeys) {
      ((FloatOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.FLOAT)).add(value);
    }
  }

  /**
   * Helper method to set DOUBLE value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, double value) {
    for (int groupKey : groupKeys) {
      ((DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.DOUBLE)).add(value);
    }
  }

  /**
   * Helper method to set STRING value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, String value) {
    for (int groupKey : groupKeys) {
      ((ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKey, DataType.STRING)).add(value);
    }
  }

  /**
   * Helper method to set BYTES value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int[] groupKeys, ByteArray value) {
    for (int groupKey : groupKeys) {
      ((ObjectOpenHashSet<ByteArray>) getValueSet(groupByResultHolder, groupKey, DataType.BYTES)).add(value);
    }
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
