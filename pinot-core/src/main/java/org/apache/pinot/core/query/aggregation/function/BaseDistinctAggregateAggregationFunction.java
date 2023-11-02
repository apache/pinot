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
import org.apache.pinot.segment.spi.datasource.NullMode;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.trace.Tracing;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * Base class for Distinct Aggregate functions that require collecting all distinct elements in a set before
 * performing the aggregate computation. This is used by DistinctSum, DistinctAvg and DistinctCount aggregation
 * functions.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public abstract class BaseDistinctAggregateAggregationFunction<T extends Comparable>
    extends BaseSingleInputAggregationFunction<Set, T> {
  private final AggregationFunctionType _functionType;
  private final NullMode _nullMode;

  protected BaseDistinctAggregateAggregationFunction(ExpressionContext expression,
      AggregationFunctionType aggregationFunctionType, NullMode nullMode) {
    super(expression);
    _functionType = aggregationFunctionType;
    _nullMode = nullMode;
  }

  @Override
  public AggregationFunctionType getType() {
    return _functionType;
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
  public Set extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      // Use empty IntOpenHashSet as a place holder for empty result
      return new IntOpenHashSet();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (Set) result;
    }
  }

  @Override
  public Set extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      // NOTE: Return an empty IntOpenHashSet for empty result.
      return new IntOpenHashSet();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      // For non-dictionary-encoded expression, directly return the value set
      return (Set) result;
    }
  }

  @Override
  public Set merge(Set intermediateResult1, Set intermediateResult2) {
    if (intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }

    Tracing.ThreadAccountantOps.sampleAndCheckInterruption();

    intermediateResult1.addAll(intermediateResult2);
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
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
   * Performs aggregation for a SV column
   */
  protected void svAggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    RoaringBitmap nullBitmap = null;

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      if (_nullMode.nullAtQueryTime()) {
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
      }
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            dictIdBitmap.add(dictIds[i]);
          }
        }
      } else {
        getDictIdBitmap(aggregationResultHolder, dictionary).addN(dictIds, 0, length);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    Set valueSet = getValueSet(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        IntOpenHashSet intSet = (IntOpenHashSet) valueSet;
        int[] intValues = blockValSet.getIntValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              intSet.add(intValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            intSet.add(intValues[i]);
          }
        }
        break;
      case LONG:
        LongOpenHashSet longSet = (LongOpenHashSet) valueSet;
        long[] longValues = blockValSet.getLongValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              longSet.add(longValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            longSet.add(longValues[i]);
          }
        }
        break;
      case FLOAT:
        FloatOpenHashSet floatSet = (FloatOpenHashSet) valueSet;
        float[] floatValues = blockValSet.getFloatValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              floatSet.add(floatValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            floatSet.add(floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) valueSet;
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              doubleSet.add(doubleValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            doubleSet.add(doubleValues[i]);
          }
        }
        break;
      case STRING:
        ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) valueSet;
        String[] stringValues = blockValSet.getStringValuesSV();
        //noinspection ManualArrayToCollectionCopy
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              stringSet.add(stringValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            stringSet.add(stringValues[i]);
          }
        }
        break;
      case BYTES:
        ObjectOpenHashSet<ByteArray> bytesSet = (ObjectOpenHashSet<ByteArray>) valueSet;
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              bytesSet.add(new ByteArray(bytesValues[i]));
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            bytesSet.add(new ByteArray(bytesValues[i]));
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a MV column
   */
  protected void mvAggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        dictIdBitmap.add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    Set valueSet = getValueSet(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        IntOpenHashSet intSet = (IntOpenHashSet) valueSet;
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int value : intValues[i]) {
            intSet.add(value);
          }
        }
        break;
      case LONG:
        LongOpenHashSet longSet = (LongOpenHashSet) valueSet;
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (long value : longValues[i]) {
            longSet.add(value);
          }
        }
        break;
      case FLOAT:
        FloatOpenHashSet floatSet = (FloatOpenHashSet) valueSet;
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (float value : floatValues[i]) {
            floatSet.add(value);
          }
        }
        break;
      case DOUBLE:
        DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) valueSet;
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (double value : doubleValues[i]) {
            doubleSet.add(value);
          }
        }
        break;
      case STRING:
        ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) valueSet;
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          //noinspection ManualArrayToCollectionCopy
          for (String value : stringValues[i]) {
            //noinspection UseBulkOperation
            stringSet.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a SV column with group by on a SV column.
   */
  protected void svAggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    RoaringBitmap nullBitmap = null;

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      if (_nullMode.nullAtQueryTime()) {
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
      }
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              ((IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT)).add(intValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            ((IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT)).add(intValues[i]);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              ((LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG)).add(longValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            ((LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG)).add(longValues[i]);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              ((FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT)).add(
                  floatValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            ((FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT)).add(floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              ((DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE)).add(
                  doubleValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            ((DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE)).add(
                doubleValues[i]);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              ((ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING)).add(
                  stringValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            ((ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING)).add(
                stringValues[i]);
          }
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        if (_nullMode.nullAtQueryTime()) {
          nullBitmap = blockValSet.getNullBitmap(_nullMode);
        }
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              ((ObjectOpenHashSet<ByteArray>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.BYTES)).add(
                  new ByteArray(bytesValues[i]));
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            ((ObjectOpenHashSet<ByteArray>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.BYTES)).add(
                new ByteArray(bytesValues[i]));
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a MV column with group by on a SV column.
   */
  protected void mvAggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          IntOpenHashSet intSet = (IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT);
          for (int value : intValues[i]) {
            intSet.add(value);
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          LongOpenHashSet longSet = (LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG);
          for (long value : longValues[i]) {
            longSet.add(value);
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          FloatOpenHashSet floatSet =
              (FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT);
          for (float value : floatValues[i]) {
            floatSet.add(value);
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          DoubleOpenHashSet doubleSet =
              (DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE);
          for (double value : doubleValues[i]) {
            doubleSet.add(value);
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          ObjectOpenHashSet<String> stringSet =
              (ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING);
          //noinspection ManualArrayToCollectionCopy
          for (String value : stringValues[i]) {
            //noinspection UseBulkOperation
            stringSet.add(value);
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a SV column with group by on a MV column.
   */
  protected void svAggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    RoaringBitmap nullBitmap = null;

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      if (_nullMode.nullAtQueryTime()) {
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
      }
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        for (int i = 0; i < length; i++) {
          if (!nullBitmap.contains(i)) {
            setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
          }
        }
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
          }
        }
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        nullBitmap = blockValSet.getNullBitmap(_nullMode);
        if (nullBitmap != null && !nullBitmap.isEmpty()) {
          for (int i = 0; i < length; i++) {
            if (!nullBitmap.contains(i)) {
              setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], new ByteArray(bytesValues[i]));
            }
          }
        } else {
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], new ByteArray(bytesValues[i]));
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Performs aggregation for a MV column with group by on a MV column.
   */
  protected void mvAggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[][] dictIds = blockValSet.getDictionaryIdsMV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store hash code of the values into the value set
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[][] intValues = blockValSet.getIntValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            IntOpenHashSet intSet = (IntOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.INT);
            for (int value : intValues[i]) {
              intSet.add(value);
            }
          }
        }
        break;
      case LONG:
        long[][] longValues = blockValSet.getLongValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            LongOpenHashSet longSet = (LongOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.LONG);
            for (long value : longValues[i]) {
              longSet.add(value);
            }
          }
        }
        break;
      case FLOAT:
        float[][] floatValues = blockValSet.getFloatValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            FloatOpenHashSet floatSet = (FloatOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.FLOAT);
            for (float value : floatValues[i]) {
              floatSet.add(value);
            }
          }
        }
        break;
      case DOUBLE:
        double[][] doubleValues = blockValSet.getDoubleValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            DoubleOpenHashSet doubleSet =
                (DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKey, DataType.DOUBLE);
            for (double value : doubleValues[i]) {
              doubleSet.add(value);
            }
          }
        }
        break;
      case STRING:
        String[][] stringValues = blockValSet.getStringValuesMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            ObjectOpenHashSet<String> stringSet =
                (ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKey, DataType.STRING);
            //noinspection ManualArrayToCollectionCopy
            for (String value : stringValues[i]) {
              //noinspection UseBulkOperation
              stringSet.add(value);
            }
          }
        }
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
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
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function valueType");
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

  /**
   * Helper method to read dictionary and convert dictionary ids to values for dictionary-encoded expression.
   */
  private static Set convertToValueSet(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    int numValues = dictIdBitmap.getCardinality();
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        IntOpenHashSet intSet = new IntOpenHashSet(numValues);
        while (iterator.hasNext()) {
          intSet.add(dictionary.getIntValue(iterator.next()));
        }
        return intSet;
      case LONG:
        LongOpenHashSet longSet = new LongOpenHashSet(numValues);
        while (iterator.hasNext()) {
          longSet.add(dictionary.getLongValue(iterator.next()));
        }
        return longSet;
      case FLOAT:
        FloatOpenHashSet floatSet = new FloatOpenHashSet(numValues);
        while (iterator.hasNext()) {
          floatSet.add(dictionary.getFloatValue(iterator.next()));
        }
        return floatSet;
      case DOUBLE:
        DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(numValues);
        while (iterator.hasNext()) {
          doubleSet.add(dictionary.getDoubleValue(iterator.next()));
        }
        return doubleSet;
      case STRING:
        ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(numValues);
        while (iterator.hasNext()) {
          stringSet.add(dictionary.getStringValue(iterator.next()));
        }
        return stringSet;
      case BYTES:
        ObjectOpenHashSet<ByteArray> bytesSet = new ObjectOpenHashSet<>(numValues);
        while (iterator.hasNext()) {
          bytesSet.add(new ByteArray(dictionary.getBytesValue(iterator.next())));
        }
        return bytesSet;
      default:
        throw new IllegalStateException("Illegal data type for DISTINCT_AGGREGATE aggregation function: " + storedType);
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
