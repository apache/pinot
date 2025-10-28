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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
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
import org.apache.pinot.spi.utils.CommonConstants;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code DistinctCountSmartHLLPlusAggregationFunction} calculates the number of distinct values for a given
 * expression (both single-valued and multi-valued are supported).
 *
 * For aggregation-only queries, the distinct values are stored in a Set initially. Once the number of distinct values
 * exceeds a threshold, the Set will be converted into a HyperLogLogPlus, and approximate result will be returned.
 *
 * The function takes an optional second argument for parameters:
 * - threshold: Threshold of the number of distinct values to trigger the conversion, 100_000 by default. Non-positive
 *              value means never convert.
 * - p: Parameter p for the converted HyperLogLogPlus, 14 by default.
 * - sp: Parameter sp for the converted HyperLogLogPlus, 0 by default.
 * Example of second argument: 'threshold=10;p=12;sp=25'
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class DistinctCountSmartHLLPlusAggregationFunction extends BaseDistinctCountSmartSketchAggregationFunction {

  private final int _threshold;
  private final int _p;
  private final int _sp;

  public DistinctCountSmartHLLPlusAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments.get(0));

    if (arguments.size() > 1) {
      Parameters parameters = new Parameters(arguments.get(1).getLiteral().getStringValue());
      _threshold = parameters._threshold;
      _p = parameters._p;
      _sp = parameters._sp;
    } else {
      _threshold = Parameters.DEFAULT_THRESHOLD;
      _p = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_P;
      _sp = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP;
    }
  }

  public int getThreshold() {
    return _threshold;
  }

  public int getP() {
    return _p;
  }

  public int getSp() {
    return _sp;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTSMARTHLLPLUS;
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

    // For non-dictionary-encoded expression, store values into the value set or HLLPlus
    if (aggregationResultHolder.getResult() instanceof HyperLogLogPlus) {
      aggregateIntoHLLPlus(length, aggregationResultHolder, blockValSet);
    } else {
      aggregateIntoSet(length, aggregationResultHolder, blockValSet);
    }
  }

  private void aggregateIntoHLLPlus(int length, AggregationResultHolder aggregationResultHolder,
                                    BlockValSet blockValSet) {
    DataType valueType = blockValSet.getValueType();
    DataType storedType = valueType.getStoredType();
    HyperLogLogPlus hllPlus = aggregationResultHolder.getResult();
    if (blockValSet.isSingleValue()) {
      switch (storedType) {
        case INT:
          int[] intValues = blockValSet.getIntValuesSV();
          for (int i = 0; i < length; i++) {
            hllPlus.offer(intValues[i]);
          }
          break;
        case LONG:
          long[] longValues = blockValSet.getLongValuesSV();
          for (int i = 0; i < length; i++) {
            hllPlus.offer(longValues[i]);
          }
          break;
        case FLOAT:
          float[] floatValues = blockValSet.getFloatValuesSV();
          for (int i = 0; i < length; i++) {
            hllPlus.offer(floatValues[i]);
          }
          break;
        case DOUBLE:
          double[] doubleValues = blockValSet.getDoubleValuesSV();
          for (int i = 0; i < length; i++) {
            hllPlus.offer(doubleValues[i]);
          }
          break;
        case STRING:
          String[] stringValues = blockValSet.getStringValuesSV();
          for (int i = 0; i < length; i++) {
            hllPlus.offer(stringValues[i]);
          }
          break;
        case BYTES:
          byte[][] bytesValues = blockValSet.getBytesValuesSV();
          for (int i = 0; i < length; i++) {
            hllPlus.offer(bytesValues[i]);
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
              hllPlus.offer(value);
            }
          }
          break;
        case LONG:
          long[][] longValues = blockValSet.getLongValuesMV();
          for (int i = 0; i < length; i++) {
            for (long value : longValues[i]) {
              hllPlus.offer(value);
            }
          }
          break;
        case FLOAT:
          float[][] floatValues = blockValSet.getFloatValuesMV();
          for (int i = 0; i < length; i++) {
            for (float value : floatValues[i]) {
              hllPlus.offer(value);
            }
          }
          break;
        case DOUBLE:
          double[][] doubleValues = blockValSet.getDoubleValuesMV();
          for (int i = 0; i < length; i++) {
            for (double value : doubleValues[i]) {
              hllPlus.offer(value);
            }
          }
          break;
        case STRING:
          String[][] stringValues = blockValSet.getStringValuesMV();
          for (int i = 0; i < length; i++) {
            for (String value : stringValues[i]) {
              hllPlus.offer(value);
            }
          }
          break;
        default:
          throw getIllegalDataTypeException(valueType, false);
      }
    }
  }

  protected HyperLogLogPlus convertSetToHLLPlus(Set valueSet, DataType storedType) {
    if (storedType == DataType.BYTES) {
      return convertByteArraySetToHLLPlus((ObjectSet<ByteArray>) valueSet);
    } else {
      return convertNonByteArraySetToHLLPlus(valueSet);
    }
  }

  protected HyperLogLogPlus convertByteArraySetToHLLPlus(ObjectSet<ByteArray> valueSet) {
    HyperLogLogPlus hllPlus = new HyperLogLogPlus(_p, _sp);
    for (ByteArray value : valueSet) {
      hllPlus.offer(value.getBytes());
    }
    return hllPlus;
  }

  protected HyperLogLogPlus convertNonByteArraySetToHLLPlus(Set valueSet) {
    HyperLogLogPlus hllPlus = new HyperLogLogPlus(_p, _sp);
    for (Object value : valueSet) {
      hllPlus.offer(value);
    }
    return hllPlus;
  }

  @Override
  public Object merge(@Nullable Object intermediateResult1, @Nullable Object intermediateResult2) {
    if (intermediateResult1 == null) {
      return intermediateResult2;
    }
    if (intermediateResult2 == null) {
      return intermediateResult1;
    }

    if (intermediateResult1 instanceof HyperLogLogPlus) {
      return mergeIntoHLLPlus((HyperLogLogPlus) intermediateResult1, intermediateResult2);
    }
    if (intermediateResult2 instanceof HyperLogLogPlus) {
      return mergeIntoHLLPlus((HyperLogLogPlus) intermediateResult2, intermediateResult1);
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

    // Convert to HLLPlus if the set size exceeds the threshold
    if (valueSet1.size() > _threshold) {
      if (valueSet1 instanceof ObjectSet && valueSet1.iterator().next() instanceof ByteArray) {
        return convertByteArraySetToHLLPlus((ObjectSet<ByteArray>) valueSet1);
      } else {
        return convertNonByteArraySetToHLLPlus(valueSet1);
      }
    } else {
      return valueSet1;
    }
  }

  private static HyperLogLogPlus mergeIntoHLLPlus(HyperLogLogPlus hllPlus, Object intermediateResult) {
    if (intermediateResult instanceof HyperLogLogPlus) {
      try {
        hllPlus.addAll((HyperLogLogPlus) intermediateResult);
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogPlus", e);
      }
    } else {
      Set valueSet = (Set) intermediateResult;
      if (!valueSet.isEmpty()) {
        if (valueSet instanceof ObjectSet && valueSet.iterator().next() instanceof ByteArray) {
          for (Object value : valueSet) {
            hllPlus.offer(((ByteArray) value).getBytes());
          }
        } else {
          for (Object value : valueSet) {
            hllPlus.offer(value);
          }
        }
      }
    }
    return hllPlus;
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public SerializedIntermediateResult serializeIntermediateResult(Object o) {
    if (o instanceof HyperLogLogPlus) {
      return new SerializedIntermediateResult(ObjectSerDeUtils.ObjectType.HyperLogLogPlus.getValue(),
          ObjectSerDeUtils.HYPER_LOG_LOG_PLUS_SER_DE.serialize((HyperLogLogPlus) o));
    }
    return BaseDistinctAggregateAggregationFunction.serializeSet((Set) o);
  }

  @Override
  public Object deserializeIntermediateResult(@Nullable CustomObject customObject) {
    return ObjectSerDeUtils.deserialize(customObject);
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(@Nullable Object intermediateResult) {
    if (intermediateResult == null) {
      return 0;
    }
    if (intermediateResult instanceof HyperLogLogPlus) {
      return (int) ((HyperLogLogPlus) intermediateResult).cardinality();
    } else {
      return ((Set) intermediateResult).size();
    }
  }

  @Override
  public Integer mergeFinalResult(@Nullable Integer finalResult1, @Nullable Integer finalResult2) {
    if (finalResult1 == null) {
      return finalResult2 == null ? 0 : finalResult2;
    }
    if (finalResult2 == null) {
      return finalResult1;
    }
    return finalResult1 + finalResult2;
  }

  /**
   * Helper method to read dictionary and convert dictionary ids to a HyperLogLogPlus for
   * dictionary-encoded expression.
   */
  private HyperLogLogPlus convertToHLLPlus(
          BaseDistinctCountSmartSketchAggregationFunction.DictIdsWrapper dictIdsWrapper) {
    HyperLogLogPlus hllPlus = new HyperLogLogPlus(_p, _sp);
    Dictionary dictionary = dictIdsWrapper._dictionary;
    RoaringBitmap dictIdBitmap = dictIdsWrapper._dictIdBitmap;
    PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
    while (iterator.hasNext()) {
      hllPlus.offer(dictionary.get(iterator.next()));
    }
    return hllPlus;
  }

  @Override
  protected Object convertSetToSketch(Set valueSet, DataType storedType) {
    return convertSetToHLLPlus(valueSet, storedType);
  }

  @Override
  protected Object convertToSketch(BaseDistinctCountSmartSketchAggregationFunction.DictIdsWrapper dictIdsWrapper) {
    return convertToHLLPlus(dictIdsWrapper);
  }

  @Override
  protected IllegalStateException getIllegalDataTypeException(DataType dataType, boolean singleValue) {
    return new IllegalStateException(
        "Illegal data type for DISTINCT_COUNT_SMART_HLL_PLUS aggregation function: " + dataType + (singleValue ? ""
            : "_MV"));
  }

  /**
   * Helper class to wrap the parameters.
   */
  private static class Parameters {
    static final char PARAMETER_DELIMITER = ';';
    static final char PARAMETER_KEY_VALUE_SEPARATOR = '=';

    static final String THRESHOLD_KEY = "THRESHOLD";
    // 100K values to trigger HLLPlus conversion by default
    static final int DEFAULT_THRESHOLD = 100_000;

    static final String P_KEY = "P";
    static final String SP_KEY = "SP";

    int _threshold = DEFAULT_THRESHOLD;
    int _p = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_P;
    int _sp = CommonConstants.Helix.DEFAULT_HYPERLOGLOG_PLUS_SP;

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
            _threshold = Integer.parseInt(value);
            // Treat non-positive threshold as unlimited
            if (_threshold <= 0) {
              _threshold = Integer.MAX_VALUE;
            }
            break;
          case P_KEY:
            _p = Integer.parseInt(value);
            break;
          case SP_KEY:
            _sp = Integer.parseInt(value);
            break;
          default:
            throw new IllegalArgumentException("Invalid parameter key: " + key);
        }
      }
    }
  }
}
