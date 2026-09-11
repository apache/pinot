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
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// This function is used for Mode calculations.
///
/// The function can be used as MODE(expression, multiModeReducerType)
///
/// Following arguments are supported:
///
/// - Expression: expression that contains the column to be calculated mode on
/// - MultiModeReducerType (optional): the reducer to use in case of multiple modes present in data
///
/// Numeric calls retain a DOUBLE result and support MIN, MAX and AVG tie reducers. The planner supplies an internal
/// third STRING or TIMESTAMP literal for non-numeric inputs, which retain their type and support MIN and MAX.
/// The result type is immutable so a function can be shared across segments and reconstructed on reducing stages.
@SuppressWarnings({"rawtypes", "unchecked"})
public class ModeAggregationFunction extends BaseSingleInputAggregationFunction<Map<?, Long>, Comparable<?>> {

  private static final double DEFAULT_FINAL_RESULT = Double.NEGATIVE_INFINITY;

  private final MultiModeReducerType _multiModeReducerType;
  private final ColumnDataType _resultType;
  private final String _resultColumnName;

  public ModeAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(checkArguments(arguments), nullHandlingEnabled);

    int numArguments = arguments.size();
    if (numArguments == 3) {
      ExpressionContext typeArgument = arguments.get(2);
      Preconditions.checkArgument(typeArgument.getType() == ExpressionContext.Type.LITERAL
              && typeArgument.getLiteral().getType() == DataType.STRING,
          "MODE result type must be a STRING or TIMESTAMP string literal");
      String resultType = typeArgument.getLiteral().getStringValue();
      Preconditions.checkArgument(resultType != null, "MODE result type must be STRING or TIMESTAMP, got: null");
      resultType = resultType.toUpperCase(Locale.ROOT);
      Preconditions.checkArgument("STRING".equals(resultType) || "TIMESTAMP".equals(resultType),
          "MODE result type must be STRING or TIMESTAMP, got: %s", resultType);
      _resultType = ColumnDataType.valueOf(resultType);
    } else {
      _resultType = ColumnDataType.DOUBLE;
    }
    if (numArguments > 1) {
      Preconditions.checkArgument(arguments.get(1).getType() == ExpressionContext.Type.LITERAL,
          "MODE tie reducer must be a literal");
      _multiModeReducerType = MultiModeReducerType.valueOf(arguments.get(1).getLiteral().getStringValue());
    } else {
      _multiModeReducerType = MultiModeReducerType.MIN;
    }
    Preconditions.checkArgument(
        _resultType == ColumnDataType.DOUBLE || _multiModeReducerType != MultiModeReducerType.AVG,
        "MODE for %s supports only MIN or MAX tie reducers, got: %s", _resultType, _multiModeReducerType);
    // Include the inferred type in the result identity while retaining legacy numeric names.
    _resultColumnName = numArguments == 3
        ? "mode(" + _expression + "," + arguments.get(1) + "," + arguments.get(2) + ")"
        : super.getResultColumnName();
  }

  private static ExpressionContext checkArguments(List<ExpressionContext> arguments) {
    Preconditions.checkArgument(!arguments.isEmpty() && arguments.size() <= 3,
        "MODE expects one to three arguments, got: %s", arguments.size());
    return arguments.get(0);
  }

  /// Helper method to create a value map for the given value type.
  private static Map<?, Long> getValueMap(DataType valueType) {
    switch (valueType) {
      case INT:
        return new Int2LongOpenHashMap();
      case LONG:
        return new Long2LongOpenHashMap();
      case FLOAT:
        return new Float2LongOpenHashMap();
      case DOUBLE:
        return new Double2LongOpenHashMap();
      case STRING:
        return new Object2LongOpenHashMap<String>();
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + valueType);
    }
  }

  /// Returns the value map from the result holder or creates a new one if it does not exist.
  private static Map<?, Long> getValueMap(AggregationResultHolder aggregationResultHolder,
      DataType valueType) {
    Map<?, Long> valueMap = aggregationResultHolder.getResult();
    if (valueMap == null) {
      valueMap = getValueMap(valueType);
      aggregationResultHolder.setValue(valueMap);
    }
    return valueMap;
  }

  /// Helper method to set INT value for the given group keys into the result holder.
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, int value) {
    Int2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Int2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /// Helper method to set LONG value for the given group keys into the result holder.
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, long value) {
    Long2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Long2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /// Helper method to set FLOAT value for the given group keys into the result holder.
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, float value) {
    Float2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Float2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  /// Helper method to set DOUBLE value for the given group keys into the result holder.
  private static void setValueForGroupKeys(GroupByResultHolder groupByResultHolder, int groupKey, double value) {
    Double2LongOpenHashMap valueMap = groupByResultHolder.getResult(groupKey);
    if (valueMap == null) {
      valueMap = new Double2LongOpenHashMap();
      groupByResultHolder.setValueForKey(groupKey, valueMap);
    }
    valueMap.merge(value, 1, Long::sum);
  }

  private static void setValueForGroupKeys(GroupByResultHolder holder, int groupKey, String value) {
    Object2LongOpenHashMap<String> counts = holder.getResult(groupKey);
    if (counts == null) {
      counts = new Object2LongOpenHashMap<>();
      holder.setValueForKey(groupKey, counts);
    }
    counts.addTo(value, 1L);
  }

  /// Returns the dictionary id count map from the result holder or creates a new one if it does not exist.
  protected static Int2IntOpenHashMap getDictIdCountMap(AggregationResultHolder aggregationResultHolder,
      Dictionary dictionary) {
    ModeAggregationFunction.DictIdsWrapper dictIdsWrapper = aggregationResultHolder.getResult();
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new ModeAggregationFunction.DictIdsWrapper(dictionary);
      aggregationResultHolder.setValue(dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdCountMap;
  }

  /// Returns the dictionary id count map for the given group key or creates a new one if it does not exist.
  protected static Int2IntOpenHashMap getDictIdCountMap(GroupByResultHolder groupByResultHolder, int groupKey,
      Dictionary dictionary) {
    ModeAggregationFunction.DictIdsWrapper dictIdsWrapper = groupByResultHolder.getResult(groupKey);
    if (dictIdsWrapper == null) {
      dictIdsWrapper = new ModeAggregationFunction.DictIdsWrapper(dictionary);
      groupByResultHolder.setValueForKey(groupKey, dictIdsWrapper);
    }
    return dictIdsWrapper._dictIdCountMap;
  }

  /// Helper method to read dictionary and convert dictionary ids to values for dictionary-encoded expression.
  private static Map<?, Long> convertToValueMap(DictIdsWrapper dictIdsWrapper) {
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
      case STRING:
        Object2LongOpenHashMap<String> stringValueMap = new Object2LongOpenHashMap<>(numValues);
        while (iterator.hasNext()) {
          Int2IntMap.Entry next = iterator.next();
          stringValueMap.put(dictionary.getStringValue(next.getIntKey()), next.getIntValue());
        }
        return stringValueMap;
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + storedType);
    }
  }

  /// Helper method to extract segment level intermediate result from the inner segment result.
  @Nullable
  private Map<?, Long> extractIntermediateResult(@Nullable Object result) {
    if (result == null) {
      // Preserve the legacy numeric empty-result sentinel.
      return _resultType == ColumnDataType.DOUBLE ? new Int2LongOpenHashMap() : null;
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
  public String getResultColumnName() {
    return _resultColumnName;
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

    // For dictionary-encoded expression, store dictionary ids into the dictId map
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {

      Int2IntOpenHashMap dictIdValueMap = getDictIdCountMap(aggregationResultHolder, dictionary);
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          dictIdValueMap.merge(dictIds[i], 1, Integer::sum);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the value map
    DataType storedType = blockValSet.getValueType().getStoredType();
    Map<?, Long> valueMap = getValueMap(aggregationResultHolder, storedType);
    switch (storedType) {
      case INT:
        Int2LongOpenHashMap intMap = (Int2LongOpenHashMap) valueMap;
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            intMap.merge(intValues[i], 1, Long::sum);
          }
        });
        break;
      case LONG:
        Long2LongOpenHashMap longMap = (Long2LongOpenHashMap) valueMap;
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            longMap.merge(longValues[i], 1, Long::sum);
          }
        });
        break;
      case FLOAT:
        Float2LongOpenHashMap floatMap = (Float2LongOpenHashMap) valueMap;
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            floatMap.merge(floatValues[i], 1, Long::sum);
          }
        });
        break;
      case DOUBLE:
        Double2LongOpenHashMap doubleMap = (Double2LongOpenHashMap) valueMap;
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            doubleMap.merge(doubleValues[i], 1, Long::sum);
          }
        });
        break;
      case STRING:
        Object2LongOpenHashMap<String> stringMap = (Object2LongOpenHashMap<String>) valueMap;
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            stringMap.addTo(stringValues[i], 1L);
          }
        });
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
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          Int2IntOpenHashMap dictIdCountMap = getDictIdCountMap(groupByResultHolder, groupKeyArray[i], dictionary);
          dictIdCountMap.merge(dictIds[i], 1, Integer::sum);
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the value map
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], longValues[i]);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], floatValues[i]);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], doubleValues[i]);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            setValueForGroupKeys(groupByResultHolder, groupKeyArray[i], stringValues[i]);
          }
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
    Dictionary dictionary = blockValSet.isDictionaryEncoded() ? blockValSet.getDictionary() : null;
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      forEachNotNull(length, blockValSet, (from, to) -> {
        for (int i = from; i < to; i++) {
          for (int groupKey : groupKeysArray[i]) {
            getDictIdCountMap(groupByResultHolder, groupKey, dictionary).merge(dictIds[i], 1, Integer::sum);
          }
        }
      });
      return;
    }

    // For non-dictionary-encoded expression, store values into the value map
    DataType storedType = blockValSet.getValueType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              setValueForGroupKeys(groupByResultHolder, groupKey, intValues[i]);
            }
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              setValueForGroupKeys(groupByResultHolder, groupKey, longValues[i]);
            }
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              setValueForGroupKeys(groupByResultHolder, groupKey, floatValues[i]);
            }
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              setValueForGroupKeys(groupByResultHolder, groupKey, doubleValues[i]);
            }
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            for (int groupKey : groupKeysArray[i]) {
              setValueForGroupKeys(groupByResultHolder, groupKey, stringValues[i]);
            }
          }
        });
        break;
      default:
        throw new IllegalStateException("Illegal data type for MODE aggregation function: " + storedType);
    }
  }

  @Nullable
  @Override
  public Map<?, Long> extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return extractIntermediateResult(aggregationResultHolder.getResult());
  }

  @Nullable
  @Override
  public Map<?, Long> extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
  }

  @Override
  public Map<?, Long> merge(Map<?, Long> intermediateResult1, Map<?, Long> intermediateResult2) {
    if (intermediateResult1.isEmpty()) {
      return intermediateResult2;
    }
    if (intermediateResult2.isEmpty()) {
      return intermediateResult1;
    }
    if (_resultType != ColumnDataType.DOUBLE) {
      Map<Object, Long> counts = (Map<Object, Long>) intermediateResult1;
      intermediateResult2.forEach((value, count) -> counts.merge(value, count, Long::sum));
      return counts;
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
  @SuppressWarnings("deprecation")
  public SerializedIntermediateResult serializeIntermediateResult(Map<?, Long> longMap) {
    if (_resultType == ColumnDataType.STRING) {
      // Reuse the existing map wire encoding for generic aggregation bridges.
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Map.getValue(),
          ObjectSerDeUtils.MAP_SER_DE.serialize((Map) longMap));
    }
    if (_resultType == ColumnDataType.TIMESTAMP && !(longMap instanceof Long2LongMap)) {
      longMap = new Long2LongOpenHashMap((Map<Long, Long>) longMap);
    }
    if (longMap instanceof Int2LongMap) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Int2LongMap.getValue(),
          ObjectSerDeUtils.INT_2_LONG_MAP_SER_DE.serialize((Int2LongMap) longMap));
    } else if (longMap instanceof Long2LongMap) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Long2LongMap.getValue(),
          ObjectSerDeUtils.LONG_2_LONG_MAP_SER_DE.serialize((Long2LongMap) longMap));
    } else if (longMap instanceof Float2LongMap) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Float2LongMap.getValue(),
          ObjectSerDeUtils.FLOAT_2_LONG_MAP_SER_DE.serialize((Float2LongMap) longMap));
    } else if (longMap instanceof Double2LongMap) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.Double2LongMap.getValue(),
          ObjectSerDeUtils.DOUBLE_2_LONG_MAP_SER_DE.serialize((Double2LongMap) longMap));
    } else {
      throw new IllegalStateException(
          "Illegal data type for Intermediate Result of MODE aggregation function: " + longMap.getClass()
              .getSimpleName());
    }
  }

  @Override
  public Map<?, Long> deserializeIntermediateResult(CustomObject customObject) {
    return ObjectSerDeUtils.deserialize(customObject);
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return _resultType;
  }

  @Nullable
  @Override
  public Comparable<?> extractFinalResult(@Nullable Map<?, Long> intermediateResult) {
    if (_resultType != ColumnDataType.DOUBLE) {
      return extractComparableFinalResult(intermediateResult);
    }
    // A null intermediate result means nothing was aggregated, and the mode of nothing is NULL. An empty map is a
    // different thing: it is what an untouched single-stage result holder produces, and it keeps its historical
    // sentinel below so that path is not silently changed.
    if (intermediateResult == null) {
      return null;
    }
    if (intermediateResult.isEmpty()) {
      if (_nullHandlingEnabled) {
        return null;
      } else {
        return DEFAULT_FINAL_RESULT;
      }
    } else if (intermediateResult instanceof Int2LongOpenHashMap) {
      return extractNumericFinalResult((Int2LongOpenHashMap) intermediateResult);
    } else if (intermediateResult instanceof Long2LongOpenHashMap) {
      return extractNumericFinalResult((Long2LongOpenHashMap) intermediateResult);
    } else if (intermediateResult instanceof Float2LongOpenHashMap) {
      return extractNumericFinalResult((Float2LongOpenHashMap) intermediateResult);
    } else if (intermediateResult instanceof Double2LongOpenHashMap) {
      return extractNumericFinalResult((Double2LongOpenHashMap) intermediateResult);
    } else {
      throw new IllegalStateException(
          "Illegal data type for Intermediate Result of MODE aggregation function: " + intermediateResult.getClass()
              .getSimpleName());
    }
  }

  private double extractNumericFinalResult(Int2LongOpenHashMap intermediateResult) {
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

  private double extractNumericFinalResult(Long2LongOpenHashMap intermediateResult) {
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

  private double extractNumericFinalResult(Float2LongOpenHashMap intermediateResult) {
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

  private Double extractNumericFinalResult(Double2LongOpenHashMap intermediateResult) {
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

  @Nullable
  private Comparable<?> extractComparableFinalResult(@Nullable Map<?, Long> counts) {
    Comparable mode = null;
    long maxCount = 0;
    if (counts != null) {
      for (Map.Entry<?, Long> entry : counts.entrySet()) {
        Comparable value = (Comparable) entry.getKey();
        long count = entry.getValue();
        if (mode == null || count > maxCount || (count == maxCount
            && (_multiModeReducerType == MultiModeReducerType.MIN
                ? value.compareTo(mode) < 0 : value.compareTo(mode) > 0))) {
          mode = value;
          maxCount = count;
        }
      }
    }
    return mode;
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
