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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Aggregation function to compute the average of distinct values for an SV column
 */
public class DistinctCountGlobalAggregationFunction extends BaseDistinctAggregateAggregationFunction<Integer> {

  private volatile Set _distinctValues;
  private final int _initialSize;

  public DistinctCountGlobalAggregationFunction(List<ExpressionContext> arguments, boolean nullHandlingEnabled) {
    super(arguments.get(0), AggregationFunctionType.DISTINCTCOUNTGLOBAL, nullHandlingEnabled);
    if (arguments.size() > 1) {
      _initialSize = arguments.get(1).getLiteral().getIntValue();
    } else {
      _initialSize = 1000;
    }
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    BlockValSet blockValSet = blockValSetMap.get(_expression);
    FieldSpec.DataType storedType = blockValSet.getValueType().getStoredType();
    initValueSetIfNeeded(aggregationResultHolder, storedType);
    // For dictionary-encoded expression, store dictionary ids into the bitmap
    Dictionary dictionary = blockValSet.getDictionary();
    if (dictionary != null) {
      int[] dictIds = blockValSet.getDictionaryIdsSV();
      switch (storedType) {
        case INT:
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              _distinctValues.add(dictionary.getIntValue(dictIds[i]));
            }
          });
          break;
        case LONG:
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              _distinctValues.add(dictionary.getLongValue(dictIds[i]));
            }
          });
          break;
        case FLOAT:
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              _distinctValues.add(dictionary.getFloatValue(dictIds[i]));
            }
          });
          break;
        case DOUBLE:
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              _distinctValues.add(dictionary.getDoubleValue(dictIds[i]));
            }
          });
          break;
        case STRING:
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              int dictId = dictIds[i];
              String stringValue = dictionary.getStringValue(dictId);
              _distinctValues.add(stringValue);
            }
          });
          break;
        case BYTES:
          forEachNotNull(length, blockValSet, (from, to) -> {
            for (int i = from; i < to; i++) {
              _distinctValues.add(new ByteArray(dictionary.getBytesValue(dictIds[i])));
            }
          });
          break;
        default:
          throw new IllegalStateException(
              "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
      }
      return;
    }
    switch (storedType) {
      case INT:
        int[] intValues = blockValSet.getIntValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            _distinctValues.add(intValues[i]);
          }
        });
        break;
      case LONG:
        long[] longValues = blockValSet.getLongValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            _distinctValues.add(longValues[i]);
          }
        });
        break;
      case FLOAT:
        float[] floatValues = blockValSet.getFloatValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            _distinctValues.add(floatValues[i]);
          }
        });
        break;
      case DOUBLE:
        double[] doubleValues = blockValSet.getDoubleValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            _distinctValues.add(doubleValues[i]);
          }
        });
        break;
      case STRING:
        String[] stringValues = blockValSet.getStringValuesSV();
        //noinspection ManualArrayToCollectionCopy
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            _distinctValues.add(stringValues[i]);
          }
        });
        break;
      case BYTES:
        byte[][] bytesValues = blockValSet.getBytesValuesSV();
        forEachNotNull(length, blockValSet, (from, to) -> {
          for (int i = from; i < to; i++) {
            _distinctValues.add(new ByteArray(bytesValues[i]));
          }
        });
        break;
      default:
        throw new IllegalStateException(
            "Illegal data type for " + _functionType.getName() + " aggregation function: " + storedType);
    }
  }

  /**
   * Initialize
   */
  protected void initValueSetIfNeeded(AggregationResultHolder aggregationResultHolder, FieldSpec.DataType valueType) {
    if (_distinctValues == null) {
      synchronized (this) {
        if (_distinctValues == null) {
          switch (valueType) {
            case INT:
              _distinctValues = new ConcurrentHashMap<Integer, Boolean>(_initialSize).keySet(Boolean.FALSE);
              break;
            case LONG:
              _distinctValues = new ConcurrentHashMap<Long, Boolean>(_initialSize).keySet(Boolean.FALSE);
              break;
            case FLOAT:
              _distinctValues = new ConcurrentHashMap<Float, Boolean>(_initialSize).keySet(Boolean.FALSE);
              break;
            case DOUBLE:
              _distinctValues = new ConcurrentHashMap<Double, Boolean>(_initialSize).keySet(Boolean.FALSE);
              break;
            case STRING:
              _distinctValues = new ConcurrentHashMap<String, Boolean>(_initialSize).keySet(Boolean.FALSE);
              break;
            case BYTES:
              _distinctValues = new ConcurrentHashMap<ByteArray, Boolean>(_initialSize).keySet(Boolean.FALSE);
              break;
            default:
              throw new IllegalStateException(
                  "Illegal data type for DISTINCT_AGGREGATE aggregation function valueType");
          }
          aggregationResultHolder.setValue(_distinctValues);
        }
      }
    }
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    svAggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    svAggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public Set merge(Set intermediateResult1, Set intermediateResult2) {
    // Should only contains one _distinctValues
    return _distinctValues;
  }

  @Override
  public Set extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return _distinctValues;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.INT;
  }

  @Override
  public Integer extractFinalResult(Set intermediateResult) {
    return _distinctValues.size();
  }

  @Override
  public Integer mergeFinalResult(Integer finalResult1, Integer finalResult2) {
    return finalResult1 + finalResult2;
  }
}
