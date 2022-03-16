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
package org.apache.pinot.core.operator.query;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountHLLAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountRawHLLAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountSmartHLLAggregationFunction;
import org.apache.pinot.segment.local.customobject.MinMaxRangePair;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.ByteArray;


/**
 * Aggregation operator that utilizes dictionary for serving aggregation queries.
 * The dictionary operator is selected in the plan maker, if the query is of aggregation type min, max, minmaxrange
 * and the column has a dictionary.
 * We don't use this operator if the segment has star tree,
 * as the dictionary will have aggregated values for the metrics, and dimensions will have star node value
 *
 * For min value, we use the first value from the dictionary
 * For max value we use the last value from dictionary
 */
@SuppressWarnings("rawtypes")
public class DictionaryBasedAggregationOperator extends BaseOperator<IntermediateResultsBlock> {
  private static final String OPERATOR_NAME = "DictionaryBasedAggregationOperator";
  private static final String EXPLAIN_NAME = "AGGREGATE_DICTIONARY";

  private final AggregationFunction[] _aggregationFunctions;
  private final Map<String, Dictionary> _dictionaryMap;
  private final int _numTotalDocs;

  public DictionaryBasedAggregationOperator(AggregationFunction[] aggregationFunctions,
      Map<String, Dictionary> dictionaryMap, int numTotalDocs) {
    _aggregationFunctions = aggregationFunctions;
    _dictionaryMap = dictionaryMap;
    _numTotalDocs = numTotalDocs;
  }

  @Override
  protected IntermediateResultsBlock getNextBlock() {
    List<Object> aggregationResults = new ArrayList<>(_aggregationFunctions.length);
    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      String column = ((ExpressionContext) aggregationFunction.getInputExpressions().get(0)).getIdentifier();
      Dictionary dictionary = _dictionaryMap.get(column);
      Object result;
      switch (aggregationFunction.getType()) {
        case MIN:
        case MINMV:
          result = toDouble(dictionary.getMinVal());
          break;
        case MAX:
        case MAXMV:
          result = toDouble(dictionary.getMaxVal());
          break;
        case MINMAXRANGE:
        case MINMAXRANGEMV:
          result = new MinMaxRangePair(toDouble(dictionary.getMinVal()), toDouble(dictionary.getMaxVal()));
          break;
        case DISTINCTCOUNT:
        case DISTINCTCOUNTMV:
          result = getDistinctValueSet(dictionary);
          break;
        case DISTINCTCOUNTHLL:
        case DISTINCTCOUNTHLLMV:
          result = getDistinctCountHLLResult(dictionary, (DistinctCountHLLAggregationFunction) aggregationFunction);
          break;
        case DISTINCTCOUNTRAWHLL:
        case DISTINCTCOUNTRAWHLLMV:
          result = getDistinctCountHLLResult(dictionary,
              ((DistinctCountRawHLLAggregationFunction) aggregationFunction).getDistinctCountHLLAggregationFunction());
          break;
        case SEGMENTPARTITIONEDDISTINCTCOUNT:
          result = (long) dictionary.length();
          break;
        case DISTINCTCOUNTSMARTHLL:
          result = getDistinctCountSmartHLLResult(dictionary,
              (DistinctCountSmartHLLAggregationFunction) aggregationFunction);
          break;
        default:
          throw new IllegalStateException(
              "Dictionary based aggregation operator does not support function type: " + aggregationFunction.getType());
      }
      aggregationResults.add(result);
    }

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctions, aggregationResults, false);
  }

  private double toDouble(Comparable value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else {
      return Double.parseDouble(value.toString());
    }
  }

  private static Set getDistinctValueSet(Dictionary dictionary) {
    int dictionarySize = dictionary.length();
    switch (dictionary.getValueType()) {
      case INT:
        IntOpenHashSet intSet = new IntOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          intSet.add(dictionary.getIntValue(dictId));
        }
        return intSet;
      case LONG:
        LongOpenHashSet longSet = new LongOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          longSet.add(dictionary.getLongValue(dictId));
        }
        return longSet;
      case FLOAT:
        FloatOpenHashSet floatSet = new FloatOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          floatSet.add(dictionary.getFloatValue(dictId));
        }
        return floatSet;
      case DOUBLE:
        DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          doubleSet.add(dictionary.getDoubleValue(dictId));
        }
        return doubleSet;
      case STRING:
        ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          stringSet.add(dictionary.getStringValue(dictId));
        }
        return stringSet;
      case BYTES:
        ObjectOpenHashSet<ByteArray> bytesSet = new ObjectOpenHashSet<>(dictionarySize);
        for (int dictId = 0; dictId < dictionarySize; dictId++) {
          bytesSet.add(new ByteArray(dictionary.getBytesValue(dictId)));
        }
        return bytesSet;
      default:
        throw new IllegalStateException();
    }
  }

  private static HyperLogLog getDistinctValueHLL(Dictionary dictionary, int log2m) {
    HyperLogLog hll = new HyperLogLog(log2m);
    int length = dictionary.length();
    for (int i = 0; i < length; i++) {
      hll.offer(dictionary.get(i));
    }
    return hll;
  }

  private static HyperLogLog getDistinctCountHLLResult(Dictionary dictionary,
      DistinctCountHLLAggregationFunction function) {
    if (dictionary.getValueType() == FieldSpec.DataType.BYTES) {
      // Treat BYTES value as serialized HyperLogLog
      try {
        HyperLogLog hll = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(dictionary.getBytesValue(0));
        int length = dictionary.length();
        for (int i = 1; i < length; i++) {
          hll.addAll(ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.deserialize(dictionary.getBytesValue(i)));
        }
        return hll;
      } catch (Exception e) {
        throw new RuntimeException("Caught exception while merging HyperLogLogs", e);
      }
    } else {
      return getDistinctValueHLL(dictionary, function.getLog2m());
    }
  }

  private static Object getDistinctCountSmartHLLResult(Dictionary dictionary,
      DistinctCountSmartHLLAggregationFunction function) {
    if (dictionary.length() > function.getHllConversionThreshold()) {
      // Store values into a HLL when the dictionary size exceeds the conversion threshold
      return getDistinctValueHLL(dictionary, function.getHllLog2m());
    } else {
      return getDistinctValueSet(dictionary);
    }
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  public List<Operator> getChildOperators() {
    return Collections.emptyList();
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
    return new ExecutionStatistics(_numTotalDocs, 0, 0, _numTotalDocs);
  }
}
