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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
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
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.utils.ByteArray;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DistinctCountSmartHLLAggregationFunction} calculates the number of distinct values for a given expression
 * (both single-valued and multi-valued are supported).
 *
 * For aggregation-only queries, the distinct values are stored in a Set initially. Once the number of distinct values
 * exceeds a threshold, the Set will be converted into a HyperLogLog, and approximate result will be returned.
 *
 * The function takes an optional second argument for parameters:
 * - threshold: Threshold of the number of distinct values to trigger the conversion, 100_000 by default. Non-positive
 *              value means never convert.
 * - log2m: Log2m for the converted HyperLogLog, 12 by default.
 * Example of second argument: 'threshold=10;log2m=8'
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountSmartHLLAggregationFunction extends BaseDistinctCountSmartSketchAggregationFunction {

  private final int _threshold;
  private final int _log2m;

  public DistinctCountSmartHLLAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));

    if (arguments.size() > 1) {
      Parameters parameters = new Parameters(arguments.get(1).getLiteral().getStringValue());
      _threshold = parameters._threshold;
      _log2m = parameters._log2m;
    } else {
      _threshold = Parameters.DEFAULT_THRESHOLD;
      _log2m = Parameters.DEFAULT_LOG2M;
    }
  }

  public int getThreshold() {
    return _threshold;
  }

  public int getLog2m() {
    return _log2m;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTSMARTHLL;
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

    // For non-dictionary-encoded expression, store values into the value set or HLL
    if (aggregationResultHolder.getResult() instanceof HyperLogLog) {
      aggregateIntoHLL(length, aggregationResultHolder, blockValSet);
    } else {
      aggregateIntoSet(length, aggregationResultHolder, blockValSet);
    }
  }

  private void aggregateIntoHLL(int length, AggregationResultHolder aggregationResultHolder, BlockValSet blockValSet) {
    DataType valueType = blockValSet.getValueType();
    DataType storedType = valueType.getStoredType();
    HyperLogLog hll = aggregationResultHolder.getResult();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            hll.offer(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            hll.offer(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            hll.offer(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            hll.offer(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            hll.offer(stringValues[i]);
          }
          break;
        case BYTES:
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            hll.offer(bytesValues[i]);
          }
          break;
        default:
          throw getIllegalDataTypeException(valueType, true);
      }
    } else {
      switch (storedType) {
        case INT:
          int[][] intValues = blockValSet.getIntValuesMV();
          for (int i = 0; i < length; i++) {
            for (int value : intValues[i]) {
              hll.offer(value);
            }
          }
          break;
        case LONG:
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            for (long value : longValues[i]) {
              hll.offer(value);
            }
          }
          break;
        case FLOAT:
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            for (float value : floatValues[i]) {
              hll.offer(value);
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            for (double value : doubleValues[i]) {
              hll.offer(value);
            }
          }
          break;
        case STRING:
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            for (String value : stringValues[i]) {
              hll.offer(value);
            }
          }
          break;
        default:
          throw getIllegalDataTypeException(valueType, false);
      }
    }
  }

  // aggregateIntoSet is handled by the base class

  protected HyperLogLog convertSetToHLL(Set valueSet, DataType storedType) {
    if (storedType == DataType.BYTES) {
      return convertByteArraySetToHLL((ObjectSet<ByteArray>) valueSet);
    } else {
      return convertNonByteArraySetToHLL(valueSet);
    }
  }

  protected HyperLogLog convertByteArraySetToHLL(ObjectSet<ByteArray> valueSet) {
    HyperLogLog hll = new HyperLogLog(_log2m);
    for (ByteArray value : valueSet) {
      hll.offer(value.getBytes());
    }
    return hll;
  }

  protected HyperLogLog convertNonByteArraySetToHLL(Set valueSet) {
    HyperLogLog hll = new HyperLogLog(_log2m);
    for (Object value : valueSet) {
      hll.offer(value);
    }
    return hll;
  }

  // group-by SV handled by the base class

  // group-by MV handled by the base class

  // extraction is handled by the base class

  // extraction is handled by the base class

  @Override
  public Object merge(Object intermediateResult1, Object intermediateResult2) {
    if (intermediateResult1 instanceof HyperLogLog) {
      return mergeIntoHLL((HyperLogLog) intermediateResult1, intermediateResult2);
    }
    if (intermediateResult2 instanceof HyperLogLog) {
      return mergeIntoHLL((HyperLogLog) intermediateResult2, intermediateResult1);
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

    // Convert to HLL if the set size exceeds the threshold
    if (valueSet1.size() > _threshold) {
      if (valueSet1 instanceof ObjectSet && valueSet1.iterator().next() instanceof ByteArray) {
        return convertByteArraySetToHLL((ObjectSet<ByteArray>) valueSet1);
      } else {
        return convertNonByteArraySetToHLL(valueSet1);
      }
    } else {
      return valueSet1;
    }
  }

  private static HyperLogLog mergeIntoHLL(HyperLogLog hll, Object intermediateResult) {
    if (intermediateResult instanceof HyperLogLog) {
      try {
        hll.addAll((HyperLogLog) intermediateResult);
      } catch (CardinalityMergeException e) {
        throw new RuntimeException("Caught exception while merging HyperLogLog", e);
      }
    } else {
      Set valueSet = (Set) intermediateResult;
      if (!valueSet.isEmpty()) {
        if (valueSet instanceof ObjectSet && valueSet.iterator().next() instanceof ByteArray) {
          for (Object value : valueSet) {
            hll.offer(((ByteArray) value).getBytes());
          }
        } else {
          for (Object value : valueSet) {
            hll.offer(value);
          }
        }
      }
    }
    return hll;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Object o) {
    if (o instanceof HyperLogLog) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.HyperLogLog.getValue(),
          ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize((HyperLogLog) o));
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
    if (intermediateResult instanceof HyperLogLog) {
      return (int) ((HyperLogLog) intermediateResult).cardinality();
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
  // helper methods for dict/value set conversions are provided by the base class

  /**
   * Helper method to read dictionary and convert dictionary ids to a HyperLogLog for dictionary-encoded expression.
   */
  private HyperLogLog convertToHLL(BaseDistinctCountSmartSketchAggregationFunction.DictIdsWrapper dictIdsWrapper) {
    HyperLogLog hyperLogLog = new HyperLogLog(_log2m);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      hyperLogLog.offer(dictionary.get(iterator.next()));
    }
    return hyperLogLog;
  }

  @Override
  protected Object convertSetToSketch(Set valueSet, DataType storedType) {
    return convertSetToHLL(valueSet, storedType);
  }

  @Override
  protected Object convertToSketch(BaseDistinctCountSmartSketchAggregationFunction.DictIdsWrapper dictIdsWrapper) {
    return convertToHLL(dictIdsWrapper);
  }

  @Override
  protected IllegalStateException getIllegalDataTypeException(DataType dataType, boolean singleValue) {
    return new IllegalStateException(
        "Illegal data type for DISTINCT_COUNT_SMART_HLL aggregation function: " + dataType + (singleValue ? ""
            : "_MV"));
  }

  /**
   * Helper class to wrap the parameters.
   */
  private static class Parameters {
    static final char PARAMETER_DELIMITER = ';';
    static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';

    static final String THRESHOLD_KEY = "THRESHOLD";
    // 100K values to trigger HLL conversion by default
    static final int DEFAULT_THRESHOLD = 100_000;
    @Deprecated
    static final String DEPRECATED_THRESHOLD_KEY = "HLLCONVERSIONTHRESHOLD";

    static final String LOG2M_KEY = "LOG2M";
    // Use 12 by default to get good accuracy for DistinctCount
    static final int DEFAULT_LOG2M = 12;
    @Deprecated
    static final String DEPRECATED_LOG2M_KEY = "HLLLOG2M";

    int _threshold = DEFAULT_THRESHOLD;
    int _log2m = DEFAULT_LOG2M;

    Parameters(String parametersString) {
      StringUtils.deleteWhitespace(parametersString);
      String[] keyValuePairs = StringUtils.split(parametersString, PARAMETER_DELIMITER);
      for (String keyValuePair : keyValuePairs) {
        String[] keyAndValue = StringUtils.split(keyValuePair, PARAMETER_KEY_VALUE_SEPARATOR);
        Preconditions.checkArgument(keyAndValue.length == 2, "Invalid parameter: %s", keyValuePair);
        String key = keyAndValue[0];
        String value = keyAndValue[1];
        switch (key.toUpperCase()) {
          case THRESHOLD_KEY:
          case DEPRECATED_THRESHOLD_KEY:
            _threshold = Integer.parseInt(value);
            // Treat non-positive threshold as unlimited
            if (_threshold <= 0) {
              _threshold = Integer.MAX_VALUE;
            }
            break;
          case LOG2M_KEY:
          case DEPRECATED_LOG2M_KEY:
            _log2m = Integer.parseInt(value);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }
  }
}
