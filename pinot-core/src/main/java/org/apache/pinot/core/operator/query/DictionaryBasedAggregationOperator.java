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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.core.operator.BaseOperator;
import org.apache.pinot.core.operator.ExecutionStatistics;
import org.apache.pinot.core.operator.blocks.IntermediateResultsBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.customobject.MinMaxRangePair;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.segment.index.readers.Dictionary;


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
    int numAggregationFunctions = _aggregationFunctions.length;
    List<Object> aggregationResults = new ArrayList<>(numAggregationFunctions);
    for (AggregationFunction aggregationFunction : _aggregationFunctions) {
      String column = ((ExpressionContext) aggregationFunction.getInputExpressions().get(0)).getIdentifier();
      Dictionary dictionary = _dictionaryMap.get(column);
      int dictionarySize = dictionary.length();
      switch (aggregationFunction.getType()) {
        case MAX:
          aggregationResults.add(dictionary.getDoubleValue(dictionarySize - 1));
          break;
        case MIN:
          aggregationResults.add(dictionary.getDoubleValue(0));
          break;
        case MINMAXRANGE:
          aggregationResults
              .add(new MinMaxRangePair(dictionary.getDoubleValue(0), dictionary.getDoubleValue(dictionarySize - 1)));
          break;
        case DISTINCTCOUNT:
          IntOpenHashSet set = new IntOpenHashSet(dictionarySize);
          switch (dictionary.getValueType()) {
            case INT:
              for (int dictId = 0; dictId < dictionarySize; dictId++) {
                set.add(dictionary.getIntValue(dictId));
              }
              break;
            case LONG:
              for (int dictId = 0; dictId < dictionarySize; dictId++) {
                set.add(Long.hashCode(dictionary.getLongValue(dictId)));
              }
              break;
            case FLOAT:
              for (int dictId = 0; dictId < dictionarySize; dictId++) {
                set.add(Float.hashCode(dictionary.getFloatValue(dictId)));
              }
              break;
            case DOUBLE:
              for (int dictId = 0; dictId < dictionarySize; dictId++) {
                set.add(Double.hashCode(dictionary.getDoubleValue(dictId)));
              }
              break;
            case STRING:
              for (int dictId = 0; dictId < dictionarySize; dictId++) {
                set.add(dictionary.getStringValue(dictId).hashCode());
              }
              break;
            case BYTES:
              for (int dictId = 0; dictId < dictionarySize; dictId++) {
                set.add(Arrays.hashCode(dictionary.getBytesValue(dictId)));
              }
              break;
            default:
              throw new IllegalStateException();
          }
          aggregationResults.add(set);
          break;
        case SEGMENTPARTITIONEDDISTINCTCOUNT:
          aggregationResults.add((long) dictionarySize);
          break;
        default:
          throw new IllegalStateException(
              "Dictionary based aggregation operator does not support function type: " + aggregationFunction.getType());
      }
    }

    // Build intermediate result block based on aggregation result from the executor.
    return new IntermediateResultsBlock(_aggregationFunctions, aggregationResults, false);
  }

  @Override
  public String getOperatorName() {
    return OPERATOR_NAME;
  }

  @Override
  public ExecutionStatistics getExecutionStatistics() {
    // NOTE: Set numDocsScanned to numTotalDocs for backward compatibility.
    return new ExecutionStatistics(_numTotalDocs, 0, 0, _numTotalDocs);
  }
}
