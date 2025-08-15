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

import com.dynatrace.hash4j.distinctcount.UltraLogLog;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.local.utils.UltraLogLogUtils;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DistinctCountSmartULLAggregationFunction} calculates the number of distinct values for a given expression
 * (both single-valued and multi-valued are supported).
 *
 * For aggregation-only queries, the distinct values are stored in a Set initially. Once the number of distinct values
 * exceeds a threshold, the Set will be converted into an UltraLogLog, and approximate result will be returned.
 *
 * The function takes an optional second argument for parameters:
 * - threshold: Threshold of the number of distinct values to trigger the conversion, 100_000 by default. Non-positive
 *              value means never convert.
 * - p: Parameter p for UltraLogLog, default defined by CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P.
 * Example of second argument: 'threshold=10;p=16'
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountSmartULLAggregationFunction extends BaseSingleInputAggregationFunction<Object, Integer> {
  // Use empty IntOpenHashSet as a placeholder for empty result
  private static final IntSet EMPTY_PLACEHOLDER = new IntOpenHashSet();

  private final int _threshold;
  private final int _p;

  public DistinctCountSmartULLAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));
    int numExpressions = arguments.size();
    // This function expects 1 or 2 arguments.
    Preconditions.checkArgument(numExpressions <= 2, "DistinctCountSmartULL expects 1 or 2 arguments, got: %s",
        numExpressions);
    if (arguments.size() > 1) {
      Parameters parameters = new Parameters(arguments.get(1).getLiteral().getStringValue());
      _threshold = parameters._threshold;
      _p = parameters._p;
    } else {
      _threshold = Parameters.DEFAULT_THRESHOLD;
      _p = org.apache.pinot.spi.utils.CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P;
    }
  }

  public int getThreshold() {
    return _threshold;
  }

  public int getP() {
    return _p;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTSMARTULL;
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
      RoaringBitmap dictIdBitmap = getDictIdBitmap(aggregationResultHolder, dictionary);
      if (blockValSet.isSingleValue()) {
        int[] dictIds = blockValSet.getDictionaryIdsSV();
        dictIdBitmap.addN(dictIds, 0, length);
      } else {
        int[][] dictIds = blockValSet.getDictionaryIdsMV();
        for (int i = 0; i < length; i++) {
          dictIdBitmap.add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set or ULL
    if (aggregationResultHolder.getResult() instanceof UltraLogLog) {
      aggregateIntoULL(length, aggregationResultHolder, blockValSet);
    } else {
      aggregateIntoSet(length, aggregationResultHolder, blockValSet);
    }
  }

  private void aggregateIntoULL(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    DataType valueType = blockValSet.getValueType();
    DataType storedType = valueType.getStoredType();
    UltraLogLog ull = aggregationResultHolder.getResult();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT: {
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            UltraLogLogUtils.hashObject(intValues[i]).ifPresent(ull::add);
          }
          break;
        }
        case LONG: {
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            UltraLogLogUtils.hashObject(longValues[i]).ifPresent(ull::add);
          }
          break;
        }
        case FLOAT: {
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            UltraLogLogUtils.hashObject(floatValues[i]).ifPresent(ull::add);
          }
          break;
        }
        case DOUBLE: {
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            UltraLogLogUtils.hashObject(doubleValues[i]).ifPresent(ull::add);
          }
          break;
        }
        case STRING: {
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            UltraLogLogUtils.hashObject(stringValues[i]).ifPresent(ull::add);
          }
          break;
        }
        case BYTES: {
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            UltraLogLogUtils.hashObject(bytesValues[i]).ifPresent(ull::add);
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, true);
      }
    } else {
      switch (storedType) {
        case INT: {
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            for (int value : intValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
          break;
        }
        case LONG: {
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            for (long value : longValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
          break;
        }
        case FLOAT: {
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            for (float value : floatValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
          break;
        }
        case DOUBLE: {
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            for (double value : doubleValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
          break;
        }
        case STRING: {
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            for (String value : stringValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
          break;
        }
        case BYTES: {
          byte[][][] bytesValues = blockValSet.getBytesValuesMV();
          for (int i = 0; i < length; i++) {
            for (byte[] value : bytesValues[i]) {
              UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
            }
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, false);
      }
    }
  }

  private void aggregateIntoSet(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    DataType valueType = blockValSet.getValueType();
    DataType storedType = valueType.getStoredType();
    Set valueSet = getValueSet(aggregationResultHolder, storedType);
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT: {
          IntOpenHashSet intSet = (IntOpenHashSet) valueSet;
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            intSet.add(intValues[i]);
          }
          break;
        }
        case LONG: {
          LongOpenHashSet longSet = (LongOpenHashSet) valueSet;
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            longSet.add(longValues[i]);
          }
          break;
        }
        case FLOAT: {
          FloatOpenHashSet floatSet = (FloatOpenHashSet) valueSet;
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            floatSet.add(floatValues[i]);
          }
          break;
        }
        case DOUBLE: {
          DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) valueSet;
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            doubleSet.add(doubleValues[i]);
          }
          break;
        }
        case STRING: {
          ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) valueSet;
          String[] stringValues = blockValSet.getStringValuesSV();
          stringSet.addAll(Arrays.asList(stringValues).subList(0, length));
          break;
        }
        case BYTES: {
          ObjectOpenHashSet<ByteArray> bytesSet = (ObjectOpenHashSet<ByteArray>) valueSet;
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            bytesSet.add(new ByteArray(bytesValues[i]));
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, true);
      }
    } else {
      switch (storedType) {
        case INT: {
          IntOpenHashSet intSet = (IntOpenHashSet) valueSet;
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            for (int value : intValues[i]) {
              intSet.add(value);
            }
          }
          break;
        }
        case LONG: {
          LongOpenHashSet longSet = (LongOpenHashSet) valueSet;
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            for (long value : longValues[i]) {
              longSet.add(value);
            }
          }
          break;
        }
        case FLOAT: {
          FloatOpenHashSet floatSet = (FloatOpenHashSet) valueSet;
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            for (float value : floatValues[i]) {
              floatSet.add(value);
            }
          }
          break;
        }
        case DOUBLE: {
          DoubleOpenHashSet doubleSet = (DoubleOpenHashSet) valueSet;
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            for (double value : doubleValues[i]) {
              doubleSet.add(value);
            }
          }
          break;
        }
        case STRING: {
          ObjectOpenHashSet<String> stringSet = (ObjectOpenHashSet<String>) valueSet;
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            Collections.addAll(stringSet, stringValues[i]);
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, false);
      }
    }

    // Convert to ULL if the set size exceeds the threshold
    if (valueSet.size() > _threshold) {
      aggregationResultHolder.setValue(convertSetToULL(valueSet, storedType));
    }
  }

  protected UltraLogLog convertSetToULL(Set valueSet, DataType storedType) {
    if (storedType == DataType.BYTES) {
      return convertByteArraySetToULL((ObjectSet<ByteArray>) valueSet);
    } else {
      return convertNonByteArraySetToULL(valueSet);
    }
  }

  protected UltraLogLog convertByteArraySetToULL(ObjectSet<ByteArray> valueSet) {
    UltraLogLog ull = UltraLogLog.create(_p);
    for (ByteArray value : valueSet) {
      UltraLogLogUtils.hashObject(value.getBytes()).ifPresent(ull::add);
    }
    return ull;
  }

  protected UltraLogLog convertNonByteArraySetToULL(Set valueSet) {
    UltraLogLog ull = UltraLogLog.create(_p);
    for (Object value : valueSet) {
      UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
    }
    return ull;
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      if (blockValSet.isSingleValue()) {
        int[] dictIds = blockValSet.getDictionaryIdsSV();
        for (int i = 0; i < length; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      } else {
        int[][] dictIds = blockValSet.getDictionaryIdsMV();
        for (int i = 0; i < length; i++) {
          getDictIdBitmap(groupByResultHolder, groupKeyArray[i], dictionary).add(dictIds[i]);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType valueType = blockValSet.getValueType();
    DataType storedType = valueType.getStoredType();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT: {
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            ((IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT)).add(intValues[i]);
          }
          break;
        }
        case LONG: {
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            ((LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG)).add(longValues[i]);
          }
          break;
        }
        case FLOAT: {
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            ((FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT)).add(floatValues[i]);
          }
          break;
        }
        case DOUBLE: {
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            ((DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE)).add(
                doubleValues[i]);
          }
          break;
        }
        case STRING: {
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            ((ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING)).add(
                stringValues[i]);
          }
          break;
        }
        case BYTES: {
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            ((ObjectOpenHashSet<ByteArray>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.BYTES)).add(
                new ByteArray(bytesValues[i]));
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, true);
      }
    } else {
      switch (storedType) {
        case INT: {
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            IntOpenHashSet intSet = (IntOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.INT);
            for (int value : intValues[i]) {
              intSet.add(value);
            }
          }
          break;
        }
        case LONG: {
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            LongOpenHashSet longSet =
                (LongOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.LONG);
            for (long value : longValues[i]) {
              longSet.add(value);
            }
          }
          break;
        }
        case FLOAT: {
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            FloatOpenHashSet floatSet =
                (FloatOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.FLOAT);
            for (float value : floatValues[i]) {
              floatSet.add(value);
            }
          }
          break;
        }
        case DOUBLE: {
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            DoubleOpenHashSet doubleSet =
                (DoubleOpenHashSet) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.DOUBLE);
            for (double value : doubleValues[i]) {
              doubleSet.add(value);
            }
          }
          break;
        }
        case STRING: {
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            ObjectOpenHashSet<String> stringSet =
                (ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKeyArray[i], DataType.STRING);
            Collections.addAll(stringSet, stringValues[i]);
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, false);
      }
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      if (blockValSet.isSingleValue()) {
        int[] dictIds = blockValSet.getDictionaryIdsSV();
        for (int i = 0; i < length; i++) {
          setDictIdForGroupKeys(groupByResultHolder, groupKeysArray[i], dictionary, dictIds[i]);
        }
      } else {
        int[][] dictIds = blockValSet.getDictionaryIdsMV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getDictIdBitmap(groupByResultHolder, groupKey, dictionary).add(dictIds[i]);
          }
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value set
    DataType valueType = blockValSet.getValueType();
    DataType storedType = valueType.getStoredType();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT: {
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], intValues[i]);
          }
          break;
        }
        case LONG: {
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], longValues[i]);
          }
          break;
        }
        case FLOAT: {
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], floatValues[i]);
          }
          break;
        }
        case DOUBLE: {
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], doubleValues[i]);
          }
          break;
        }
        case STRING: {
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], stringValues[i]);
          }
          break;
        }
        case BYTES: {
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeysArray[i], new ByteArray(bytesValues[i]));
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, true);
      }
    } else {
      switch (storedType) {
        case INT: {
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
        }
        case LONG: {
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
        }
        case FLOAT: {
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
        }
        case DOUBLE: {
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
        }
        case STRING: {
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            for (int groupKey : groupKeysArray[i]) {
              ObjectOpenHashSet<String> stringSet =
                  (ObjectOpenHashSet<String>) getValueSet(groupByResultHolder, groupKey, DataType.STRING);
              Collections.addAll(stringSet, stringValues[i]);
            }
          }
          break;
        }
        default:
          throw getIllegalDataTypeException(valueType, false);
      }
    }
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    Object result = aggregationResultHolder.getResult();
    if (result == null) {
      return EMPTY_PLACEHOLDER;
    }

    if (result instanceof DictIdsWrapper) {
      DictIdsWrapper dictIdsWrapper = (DictIdsWrapper) result;
      if (dictIdsWrapper._dictIdBitmap.cardinalityExceeds(_threshold)) {
        return convertToULL(dictIdsWrapper);
      } else {
        return convertToValueSet(dictIdsWrapper);
      }
    } else {
      return result;
    }
  }

  @Override
  public Set extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    Object result = groupByResultHolder.getResult(groupKey);
    if (result == null) {
      return EMPTY_PLACEHOLDER;
    }

    if (result instanceof DictIdsWrapper) {
      return convertToValueSet((DictIdsWrapper) result);
    } else {
      return (Set) result;
    }
  }

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    if (intermediateResult1 instanceof UltraLogLog) {
      return mergeIntoULL((UltraLogLog) intermediateResult1, intermediateResult2);
    }
    if (intermediateResult2 instanceof UltraLogLog) {
      return mergeIntoULL((UltraLogLog) intermediateResult2, intermediateResult1);
    }

    Set valueSet1 = (Set) intermediateResult1;
    Set valueSet2 = (Set) intermediateResult2;
    if (valueSet1.isEmpty()) {
      return valueSet2;
    }
    if (valueSet2.isEmpty()) {
      return valueSet1;
    }
    valueSet1.addAll(valueSet2);

    if (valueSet1.size() > _threshold) {
      if (valueSet1 instanceof ObjectSet && valueSet1.iterator().next() instanceof ByteArray) {
        return convertByteArraySetToULL((ObjectSet<ByteArray>) valueSet1);
      } else {
        return convertNonByteArraySetToULL(valueSet1);
      }
    } else {
      return valueSet1;
    }
  }

  private UltraLogLog mergeIntoULL(UltraLogLog ull, Object intermediateResult) {
    if (intermediateResult instanceof UltraLogLog) {
      UltraLogLog other = (UltraLogLog) intermediateResult;
      int largerP = Math.max(ull.getP(), other.getP());
      if (largerP != ull.getP()) {
        UltraLogLog merged = UltraLogLog.create(largerP);
        merged.add(ull);
        merged.add(other);
        return merged;
      } else {
        ull.add(other);
        return ull;
      }
    } else {
      Set valueSet = (Set) intermediateResult;
      if (!valueSet.isEmpty()) {
        if (valueSet instanceof ObjectSet && valueSet.iterator().next() instanceof ByteArray) {
          for (Object value : valueSet) {
            UltraLogLogUtils.hashObject(((ByteArray) value).getBytes()).ifPresent(ull::add);
          }
        } else {
          for (Object value : valueSet) {
            UltraLogLogUtils.hashObject(value).ifPresent(ull::add);
          }
        }
      }
      return ull;
    }
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Object o) {
    if (o instanceof UltraLogLog) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.UltraLogLog.getValue(),
          ObjectSerDeUtils.ULTRA_LOG_LOG_OBJECT_SER_DE.serialize((UltraLogLog) o));
    }
    return BaseDistinctAggregateAggregationFunction.serializeSet((Set) o);
  }

  @Override
  public Object deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.deserialize(customObject);
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(Object intermediateResult) {
    if (intermediateResult instanceof UltraLogLog) {
      return (int) Math.round(((UltraLogLog) intermediateResult).getDistinctCountEstimate());
    } else {
      return ((Set) intermediateResult).size();
    }
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return finalResult1 + finalResult2;
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
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_SMART_ULL aggregation function: " + valueType);
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
   * Helper method to read dictionary and convert dictionary ids to a value set for dictionary-encoded expression.
   */
  private static Set convertToValueSet(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    int numValues = dictIdBitmap.getCardinality();
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT: {
        IntOpenHashSet intSet = new IntOpenHashSet(numValues);
        while (iterator.hasNext()) {
          intSet.add(dictionary.getIntValue(iterator.next()));
        }
        return intSet;
      }
      case LONG: {
        LongOpenHashSet longSet = new LongOpenHashSet(numValues);
        while (iterator.hasNext()) {
          longSet.add(dictionary.getLongValue(iterator.next()));
        }
        return longSet;
      }
      case FLOAT: {
        FloatOpenHashSet floatSet = new FloatOpenHashSet(numValues);
        while (iterator.hasNext()) {
          floatSet.add(dictionary.getFloatValue(iterator.next()));
        }
        return floatSet;
      }
      case DOUBLE: {
        DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(numValues);
        while (iterator.hasNext()) {
          doubleSet.add(dictionary.getDoubleValue(iterator.next()));
        }
        return doubleSet;
      }
      case STRING: {
        ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(numValues);
        while (iterator.hasNext()) {
          stringSet.add(dictionary.getStringValue(iterator.next()));
        }
        return stringSet;
      }
      case BYTES: {
        ObjectOpenHashSet<ByteArray> bytesSet = new ObjectOpenHashSet<>(numValues);
        while (iterator.hasNext()) {
          bytesSet.add(new ByteArray(dictionary.getBytesValue(iterator.next())));
        }
        return bytesSet;
      }
      default:
        throw new IllegalStateException(
            "Illegal data type for DISTINCT_COUNT_SMART_ULL aggregation function: " + storedType);
    }
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to an UltraLogLog for dictionary-encoded expression.
   */
  private UltraLogLog convertToULL(DictIdsWrapper dictIdsWrapper) {
    UltraLogLog ull = UltraLogLog.create(_p);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      UltraLogLogUtils.hashObject(dictionary.get(iterator.next())).ifPresent(ull::add);
    }
    return ull;
  }

  private static IllegalStateException getIllegalDataTypeException(DataType dataType, boolean singleValue) {
    return new IllegalStateException(
        "Illegal data type for DISTINCT_COUNT_SMART_ULL aggregation function: " + dataType + (singleValue ? ""
            : "_MV"));
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap _dictIdBitmap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdBitmap = new RoaringBitmap();
    }
  }

  /**
   * Helper class to wrap the parameters.
   */
  private static class Parameters {
    static final char PARAMETER_DELIMITER = ';';
    static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';

    static final String THRESHOLD_KEY = "THRESHOLD";
    static final int DEFAULT_THRESHOLD = 100_000;

    static final String P_KEY = "P";

    int _threshold = DEFAULT_THRESHOLD;
    int _p = org.apache.pinot.spi.utils.CommonConstants.Helix.DEFAULT_ULTRALOGLOG_P;

    Parameters(String parametersString) {
      parametersString = StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        switch (key.toUpperCase()) {
          case THRESHOLD_KEY:
            _threshold = Integer.parseInt(value);
            if (_threshold <= 0) {
              _threshold = Integer.MAX_VALUE;
            }
            break;
          case P_KEY:
            _p = Integer.parseInt(value);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }
  }
}
