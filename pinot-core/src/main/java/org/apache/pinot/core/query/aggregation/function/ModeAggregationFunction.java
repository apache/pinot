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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import it.unimi.dsi.fastutil.doubles.Double2LongOpenHashMap;
import it.unimi.dsi.fastutil.floats.Float2LongMap;
import it.unimi.dsi.fastutil.floats.Float2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
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


/**
 * This function is used for Mode calculations.
 * <p>The function can be used as MODE(expression, multiModeReducerType)
 * <p>Following arguments are supported:
 * <ul>
 *   <li>Expression: expression that contains the column to be calculated mode on, can be any Numeric column</li>
 *   <li>MultiModeReducerType (optional): the reducer to use in case of multiple modes present in data</li>
 * </ul>
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ModeAggregationFunction
    extends NullableSingleInputAggregationFunction<Map<? extends Number, Long>, Double> {

  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  private final MultiModeReducerType _multiModeReducerType;

  public ModeAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), nullHandlingEnabled);

    int numArguments = arguments.size();
    Preconditions.checkArgument(numArguments <= 2, "Mode expects at most 2 arguments, got: %s", numArguments);
    if (numArguments > 1) {
      _multiModeReducerType = MultiModeReducerType.valueOf(arguments.get(1).getLiteral().getStringValue());
    } else {
      _multiModeReducerType = MultiModeReducerType.MIN;
    }
  }

  /**
   * Helper method to create a value map for the given value type.
   */
  private static Map<? extends Number, Long> getValueMap(DataType valueType) {
    switch (valueType) {
      case INT:
        return new Int2LongOpenHashMap();
      case LONG:
        return new Long2LongOpenHashMap();
      case FLOAT:
        return new Float2LongOpenHashMap();
      case DOUBLE:
        return new Double2LongOpenHashMap();
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + valueType);
    }
  }

  /**
   * Returns the value map from the result holder or creates a new one if it does not exist.
   */
  private static Map<? extends Number, Long> getValueMap(AggregationResultHolder aggregationResultHolder,
      DataType valueType) {
    Map<? extends Number, Long> valueMap = aggregationResultHolder.getResult();
    if (valueMap == null) {
      valueMap = getValueMap(valueType);
      aggregationResultHolder.setValue(valueMap);
    }
    return valueMap;
  }

  /**
   * Helper method to set INT value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, int value) {
    Int2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Int2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /**
   * Helper method to set LONG value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, long value) {
    Long2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Long2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /**
   * Helper method to set FLOAT value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, float value) {
    Float2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Float2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /**
   * Helper method to set DOUBLE value for the given group keys into the result holder.
   */
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, double value) {
    Double2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Double2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /**
   * Returns the dictionary id count map from the result holder or creates a new one if it does not exist.
   */
  protected static Int2IntOpenHashMap getDictIdCountMap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    ModeAggregationFunction.DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new ModeAggregationFunction.DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdCountMap;
  }

  /**
   * Returns the dictionary id count map for the given group key or creates a new one if it does not exist.
   */
  protected static Int2IntOpenHashMap getDictIdCountMap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    ModeAggregationFunction.DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new ModeAggregationFunction.DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdCountMap;
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to values for dictionary-encoded expression.
   */
  private static Map<? extends Number, Long> convertToValueMap(DictIdsWrapper dictIdsWrapper) {
    Dictionary dictionary = dictIdsWrapper._dictionary;
    Int2IntOpenHashMap dictIdCountMap = dictIdsWrapper._dictIdCountMap;
    int numValues = dictIdCountMap.size();
    ObjectIterator<Int2IntMap.Entry> iterator = Int2IntMaps.fastIterator(dictIdCountMap);
    DataType storedType = dictionary.getValueType();
    switch (storedType) {
      case INT:
        Int2LongOpenHashMap intValueMap = new Int2LongOpenHashMap(numValues);
        while (iterator.hasNext()) {
          Int2IntMap.Entry next = iterator.next();
          intValueMap.put(dictionary.getIntValue(next.getIntKey()), next.getIntValue());
        }
        return intValueMap;
      case LONG:
        Long2LongOpenHashMap longValueMap = new Long2LongOpenHashMap(numValues);
        while (iterator.hasNext()) {
          Int2IntMap.Entry next = iterator.next();
          longValueMap.put(dictionary.getLongValue(next.getIntKey()), next.getIntValue());
        }
        return longValueMap;
      case FLOAT:
        Float2LongOpenHashMap floatValueMap = new Float2LongOpenHashMap(numValues);
        while (iterator.hasNext()) {
          Int2IntMap.Entry next = iterator.next();
          floatValueMap.put(dictionary.getFloatValue(next.getIntKey()), next.getIntValue());
        }
        return floatValueMap;
      case DOUBLE:
        Double2LongOpenHashMap doubleValueMap = new Double2LongOpenHashMap(numValues);
        while (iterator.hasNext()) {
          Int2IntMap.Entry next = iterator.next();
          doubleValueMap.put(dictionary.getDoubleValue(next.getIntKey()), next.getIntValue());
        }
        return doubleValueMap;
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + storedType);
    }
  }

  /**
   * Helper method to extract segment level intermediate result from the inner segment result.
   */
  private static Map<? extends Number, Long> extractIntermediateResult(@Nullable Object result) {
    if (result == null) {
      // NOTE: Return an empty Int2LongOpenHashMap for empty result.
      return new Int2LongOpenHashMap();
    }

    if (result instanceof DictIdsWrapper) {
      // For dictionary-encoded expression, convert dictionary ids to values
      return convertToValueMap((DictIdsWrapper) result);
    }
    assert result instanceof Map;
    // For non-dictionary-encoded expression, directly return the value set
    return (Map) result;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.MODE;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  protected void onAggregationOnlyDict(int length, AggregationResultHolder aggregationResultHolder,
      BlockValSet blockValSet, Dictionary dictionary) {
    Int2IntOpenHashMap dictIdValueMap = getDictIdCountMap(aggregationResultHolder, dictionary);
    forEachNotNullDictId(length, blockValSet, did -> dictIdValueMap.merge(did, 1, Integer::sum));
  }

  protected void onAggregationOnlyInt(int length, Map<? extends Number, Long> valueMap, BlockValSet blockValSet) {
    Int2LongOpenHashMap intMap = (Int2LongOpenHashMap) valueMap;
    forEachNotNullInt(length, blockValSet, value -> intMap.merge(value, 1, Long::sum));
  }

  protected void onAggregationOnlyLong(int length, Map<? extends Number, Long> valueMap, BlockValSet blockValSet) {
    Long2LongOpenHashMap longMap = (Long2LongOpenHashMap) valueMap;
    forEachNotNullLong(length, blockValSet, value -> longMap.merge(value, 1, Long::sum));
  }

  protected void onAggregationOnlyFloat(int length, Map<? extends Number, Long> valueMap, BlockValSet blockValSet) {
    Float2LongOpenHashMap floatMap = (Float2LongOpenHashMap) valueMap;
    forEachNotNullFloat(length, blockValSet, value -> floatMap.merge(value, 1, Long::sum));
  }

  protected void onAggregationOnlyDouble(int length, Map<? extends Number, Long> valueMap, BlockValSet blockValSet) {
    Double2LongOpenHashMap doubleMap = (Double2LongOpenHashMap) valueMap;
    forEachNotNullDouble(length, blockValSet, value -> doubleMap.merge(value, 1, Long::sum));
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the dictId map
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      onAggregationOnlyDict(length, aggregationResultHolder, blockValSet, dictionary);
      return;
    }

    // For non-dictionary-encoded expression, store values into the value map
    DataType storedType = blockValSet.getValueType().getStoredType();
    Map<? extends Number, Long> valueMap = getValueMap(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        onAggregationOnlyInt(length, valueMap, blockValSet);
        break;
      case LONG:
        onAggregationOnlyLong(length, valueMap, blockValSet);
        break;
      case FLOAT:
        onAggregationOnlyFloat(length, valueMap, blockValSet);
        break;
      case DOUBLE:
        onAggregationOnlyDouble(length, valueMap, blockValSet);
        break;
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the dictId map
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      for (int i = 0; i < length; i++) {
        Int2IntOpenHashMap dictIdCountMap = getDictIdCountMap(groupByResultHolder, groupKeyArray[i], dictionary);
        forEachNotNullDictId(length, blockValSet, did -> dictIdCountMap.merge(did, 1, Integer::sum));
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value map
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        forEachNotNullInt(length, blockValSet, (i, value) -> {
          setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], value);
        });
        break;
      case LONG:
        forEachNotNullLong(length, blockValSet, (i, value) -> {
          setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], value);
        });
        break;
      case FLOAT:
        forEachNotNullFloat(length, blockValSet, (i, value) -> {
          setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], value);
        });
        break;
      case DOUBLE:
        forEachNotNullDouble(length, blockValSet, (i, value) -> {
          setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], value);
        });
        break;
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + storedType);
    }
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);

    // For dictionary-encoded expression, store dictionary ids into the dictId map
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      for (int i = 0; i < length; i++) {
        for (int groupKey : groupKeysArray[i]) {
          getDictIdCountMap(groupByResultHolder, groupKey, dictionary).merge(dictIds[i], 1, Integer::sum);
        }
      }
      return;
    }

    // For non-dictionary-encoded expression, store values into the value map
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            setValueForGroupKeys(groupByResultHolder, groupKey, intValues[i]);
          }
        }
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            setValueForGroupKeys(groupByResultHolder, groupKey, longValues[i]);
          }
        }
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            setValueForGroupKeys(groupByResultHolder, groupKey, floatValues[i]);
          }
        }
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        for (int i = 0; i < length; i++) {
          for (int groupKey : groupKeysArray[i]) {
            setValueForGroupKeys(groupByResultHolder, groupKey, doubleValues[i]);
          }
        }
        break;
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + storedType);
    }
  }

  @Override
  public Map<? extends Number, Long> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return extractIntermediateResult(aggregationResultHolder.getResult());
  }

  @Override
  public Map<? extends Number, Long> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
  }

  @Override
  public Map<? extends Number, Long> merge(Map<? extends Number, Long> intermediateResult1,
      Map<? extends Number, Long> intermediateResult2) {
    if (intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    if (intermediateResult1 instanceof Int2LongOpenHashMap && intermediateResult2 instanceof Int2LongOpenHashMap) {
      ((Int2LongOpenHashMap) intermediateResult2).int2LongEntrySet().fastForEach(
          e -> ((Int2LongOpenHashMap) intermediateResult1).merge(e.getIntKey(), e.getLongValue(), Long::sum));
    } else if (intermediateResult1 instanceof Long2LongOpenHashMap
        && intermediateResult2 instanceof Long2LongOpenHashMap) {
      ((Long2LongOpenHashMap) intermediateResult2).long2LongEntrySet().fastForEach(
          e -> ((Long2LongOpenHashMap) intermediateResult1).merge(e.getLongKey(), e.getLongValue(), Long::sum));
    } else if (intermediateResult1 instanceof Float2LongOpenHashMap
        && intermediateResult2 instanceof Float2LongOpenHashMap) {
      ((Float2LongOpenHashMap) intermediateResult2).float2LongEntrySet().fastForEach(
          e -> ((Float2LongOpenHashMap) intermediateResult1).merge(e.getFloatKey(), e.getLongValue(), Long::sum));
    } else if (intermediateResult1 instanceof Double2LongOpenHashMap
        && intermediateResult2 instanceof Double2LongOpenHashMap) {
      ((Double2LongOpenHashMap) intermediateResult2).double2LongEntrySet().fastForEach(
          e -> ((Double2LongOpenHashMap) intermediateResult1).merge(e.getDoubleKey(), e.getLongValue(), Long::sum));
    } else {
      throw new IllegalStateException(
          "Illegal data type for Intermediate Result of MODE aggregation function: " + intermediateResult1.getClass()
              .getSimpleName() + ", " + intermediateResult2.getClass().getSimpleName());
    }
    return intermediateResult1;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(Map<? extends Number, Long> intermediateResult) {
    if (intermediateResult.isEmpty()) {
      return DEFAULT_FINAL_RESULT;
    } else if (intermediateResult instanceof Int2LongOpenHashMap) {
      return extractFinalResult((Int2LongOpenHashMap) intermediateResult);
    } else if (intermediateResult instanceof Long2LongOpenHashMap) {
      return extractFinalResult((Long2LongOpenHashMap) intermediateResult);
    } else if (intermediateResult instanceof Float2LongOpenHashMap) {
      return extractFinalResult((Float2LongOpenHashMap) intermediateResult);
    } else if (intermediateResult instanceof Double2LongOpenHashMap) {
      return extractFinalResult((Double2LongOpenHashMap) intermediateResult);
    } else {
      throw new IllegalStateException(
          "Illegal data type for Intermediate Result of MODE aggregation function: " + intermediateResult.getClass()
              .getSimpleName());
    }
  }

  public double extractFinalResult(Int2LongOpenHashMap intermediateResult) {
    ObjectIterator<Int2LongMap.Entry> iterator = intermediateResult.int2LongEntrySet().fastIterator();
    Int2LongMap.Entry first = iterator.next();
    long maxFrequency = first.getLongValue();
    switch (_multiModeReducerType) {
      case MIN:
        int min = first.getIntKey();
        while (iterator.hasNext()) {
          Int2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && min > next.getIntKey())) {
            maxFrequency = next.getLongValue();
            min = next.getIntKey();
          }
        }
        return min;
      case MAX:
        int max = first.getIntKey();
        while (iterator.hasNext()) {
          Int2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && max < next.getIntKey())) {
            maxFrequency = next.getLongValue();
            max = next.getIntKey();
          }
        }
        return max;
      case AVG:
        double sum = first.getIntKey();
        int count = 1;
        while (iterator.hasNext()) {
          Int2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency)) {
            maxFrequency = next.getLongValue();
            sum = next.getIntKey();
            count = 1;
          } else if (next.getLongValue() == maxFrequency) {
            sum += next.getIntKey();
            count += 1;
          }
        }
        return sum / count;
      default:
        throw new IllegalStateException("Illegal reducer type for MODE aggregation function: " + _multiModeReducerType);
    }
  }

  public double extractFinalResult(Long2LongOpenHashMap intermediateResult) {
    ObjectIterator<Long2LongMap.Entry> iterator = intermediateResult.long2LongEntrySet().fastIterator();
    Long2LongMap.Entry first = iterator.next();
    long maxFrequency = first.getLongValue();
    switch (_multiModeReducerType) {
      case MIN:
        long min = first.getLongKey();
        while (iterator.hasNext()) {
          Long2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && min > next
              .getLongKey())) {
            maxFrequency = next.getLongValue();
            min = next.getLongKey();
          }
        }
        return min;
      case MAX:
        long max = first.getLongKey();
        while (iterator.hasNext()) {
          Long2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && max < next
              .getLongKey())) {
            maxFrequency = next.getLongValue();
            max = next.getLongKey();
          }
        }
        return max;
      case AVG:
        double sum = first.getLongKey();
        int count = 1;
        while (iterator.hasNext()) {
          Long2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency)) {
            maxFrequency = next.getLongValue();
            sum = next.getLongKey();
            count = 1;
          } else if (next.getLongValue() == maxFrequency) {
            sum += next.getLongKey();
            count += 1;
          }
        }
        return sum / count;
      default:
        throw new IllegalStateException("Illegal reducer type for MODE aggregation function: " + _multiModeReducerType);
    }
  }

  public double extractFinalResult(Float2LongOpenHashMap intermediateResult) {
    ObjectIterator<Float2LongMap.Entry> iterator = intermediateResult.float2LongEntrySet().fastIterator();
    Float2LongMap.Entry first = iterator.next();
    long maxFrequency = first.getLongValue();
    switch (_multiModeReducerType) {
      case MIN:
        float min = first.getFloatKey();
        while (iterator.hasNext()) {
          Float2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && min > next
              .getFloatKey())) {
            maxFrequency = next.getLongValue();
            min = next.getFloatKey();
          }
        }
        return min;
      case MAX:
        float max = first.getFloatKey();
        while (iterator.hasNext()) {
          Float2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && max < next
              .getFloatKey())) {
            maxFrequency = next.getLongValue();
            max = next.getFloatKey();
          }
        }
        return max;
      case AVG:
        double sum = first.getFloatKey();
        int count = 1;
        while (iterator.hasNext()) {
          Float2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency)) {
            maxFrequency = next.getLongValue();
            sum = next.getFloatKey();
            count = 1;
          } else if (next.getLongValue() == maxFrequency) {
            sum += next.getFloatKey();
            count += 1;
          }
        }
        return sum / count;
      default:
        throw new IllegalStateException("Illegal reducer type for MODE aggregation function: " + _multiModeReducerType);
    }
  }

  public Double extractFinalResult(Double2LongOpenHashMap intermediateResult) {
    ObjectIterator<Double2LongMap.Entry> iterator = intermediateResult.double2LongEntrySet().fastIterator();
    Double2LongMap.Entry first = iterator.next();
    long maxFrequency = first.getLongValue();
    switch (_multiModeReducerType) {
      case MIN:
        double min = first.getDoubleKey();
        while (iterator.hasNext()) {
          Double2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && min > next
              .getDoubleKey())) {
            maxFrequency = next.getLongValue();
            min = next.getDoubleKey();
          }
        }
        return min;
      case MAX:
        double max = first.getDoubleKey();
        while (iterator.hasNext()) {
          Double2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency) || (next.getLongValue() == maxFrequency && max < next
              .getDoubleKey())) {
            maxFrequency = next.getLongValue();
            max = next.getDoubleKey();
          }
        }
        return max;
      case AVG:
        double sum = first.getDoubleKey();
        int count = 1;
        while (iterator.hasNext()) {
          Double2LongMap.Entry next = iterator.next();
          if ((next.getLongValue() > maxFrequency)) {
            maxFrequency = next.getLongValue();
            sum = next.getDoubleKey();
            count = 1;
          } else if (next.getLongValue() == maxFrequency) {
            sum += next.getDoubleKey();
            count += 1;
          }
        }
        return sum / count;
      default:
        throw new IllegalStateException("Illegal reducer type for MODE aggregation function: " + _multiModeReducerType);
    }
  }

  private enum MultiModeReducerType {
    MIN, MAX, AVG
  }

  private static final class DictIdsWrapper {

    final Dictionary _dictionary;
    final Int2IntOpenHashMap _dictIdCountMap;

    private DictIdsWrapper(Dictionary dictionary) {
      _dictionary = dictionary;
      _dictIdCountMap = new Int2IntOpenHashMap();
    }
  }
}
