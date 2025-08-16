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
import it.unimi.dsi.fastutil.objects.ObjectSet;
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
public class DistinctCountSmartULLAggregationFunction extends BaseDistinctCountSmartSketchAggregationFunction {
  // placeholder handled by base class

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

  // Result holder creators are provided by the base class

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

  // aggregateIntoSet is handled by the base class

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

  // group-by SV handled by the base class

  // group-by MV handled by the base class

  // extraction is handled by the base class

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
  // getDictIdBitmap for result holder is provided by the base class

  /**
   * Returns the value set from the result holder or creates a new one if it does not exist.
   */
  // value set helpers are provided by the base class

  /**
   * Returns the dictionary id bitmap for the given group key or creates a new one if it does not exist.
   */
  // groupBy result helpers are provided by the base class

  /**
   * Returns the value set for the given group key or creates a new one if it does not exist.
   */
  // group-by value set helper is provided by the base class

  /**
   * Helper method to set dictionary id for the given group keys into the result holder.
   */
  // setDictIdForGroupKeys is provided by the base class

  /**
   * Helper method to set INT value for the given group keys into the result holder.
   */
  // typed setValueForGroupKeys helpers are provided by the base class

  /**
   * Helper method to set LONG value for the given group keys into the result holder.
   */

  /**
   * Helper method to set FLOAT value for the given group keys into the result holder.
   */

  /**
   * Helper method to set DOUBLE value for the given group keys into the result holder.
   */

  /**
   * Helper method to set STRING value for the given group keys into the result holder.
   */

  /**
   * Helper method to set BYTES value for the given group keys into the result holder.
   */
  // typed setValueForGroupKeys helpers are provided by the base class

  /**
   * Helper method to read dictionary and convert dictionary ids to a value set for dictionary-encoded expression.
   */
  // value set conversion handled by the base class

  /**
   * Helper method to read dictionary and convert dictionary ids to an UltraLogLog for dictionary-encoded expression.
   */
  private UltraLogLog convertToULL(BaseDistinctCountSmartSketchAggregationFunction.DictIdsWrapper dictIdsWrapper) {
    UltraLogLog ull = UltraLogLog.create(_p);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      UltraLogLogUtils.hashObject(dictionary.get(iterator.next())).ifPresent(ull::add);
    }
    return ull;
  }

  @Override
  protected IllegalStateException getIllegalDataTypeException(DataType dataType, boolean singleValue) {
    return new IllegalStateException(
        "Illegal data type for DISTINCT_COUNT_SMART_ULL aggregation function: " + dataType + (singleValue ? ""
            : "_MV"));
  }

  // DictIdsWrapper is provided by the base class

  // threshold accessor for base class is provided by getThreshold()

  @Override
  protected Object convertSetToSketch(Set valueSet, DataType storedType) {
    return convertSetToULL(valueSet, storedType);
  }

  @Override
  protected Object convertToSketch(BaseDistinctCountSmartSketchAggregationFunction.DictIdsWrapper dictIdsWrapper) {
    return convertToULL(dictIdsWrapper);
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
