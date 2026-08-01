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
package org.apache.pinot.core.query.aggregation;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultAggregationExecutor implements AggregationExecutor {
  protected final AggregationFunction[] _aggregationFunctions;
  protected final AggregationResultHolder[] _aggregationResultHolders;
  @Nullable
  protected final Object[] _nonScanResults;

  /// Creates an executor that skips functions with a non-scan result (resolved from the column dictionary or
  /// metadata). For each index {@code i} where {@code nonScanResults[i]} is non-null, the function is not aggregated
  /// over the scanned blocks and the resolved value is emitted directly in the results. A {@code null} array disables
  /// this behavior and all functions are computed by scanning.
  ///
  /// @param nonScanResults per-function results resolved without scanning (from the column dictionary or metadata),
  ///     or {@code null} if none are resolved
  public DefaultAggregationExecutor(AggregationFunction[] aggregationFunctions,
      @Nullable Object[] nonScanResults) {
    _nonScanResults = nonScanResults;
    _aggregationFunctions = aggregationFunctions;
    int numAggregationFunctions = aggregationFunctions.length;
    _aggregationResultHolders = new AggregationResultHolder[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationResultHolders[i] = aggregationFunctions[i].createAggregationResultHolder();
    }
  }

  public DefaultAggregationExecutor(AggregationFunction[] aggregationFunctions) {
    this(aggregationFunctions, null);
  }

  @Override
  public void aggregate(ValueBlock valueBlock) {
    int numAggregationFunctions = _aggregationFunctions.length;
    int length = valueBlock.getNumDocs();
    for (int i = 0; i < numAggregationFunctions; i++) {
      if (_nonScanResults != null && _nonScanResults[i] != null) {
        continue; // skip — already resolved without scanning (dictionary/metadata)
      }
      AggregationFunction aggregationFunction = _aggregationFunctions[i];
      aggregationFunction.aggregate(length, _aggregationResultHolders[i],
          AggregationFunctionUtils.getBlockValSetMap(aggregationFunction, valueBlock));
    }
  }

  @Override
  public List<Object> getResult() {
    int numFunctions = _aggregationFunctions.length;
    List<Object> aggregationResults = new ArrayList<>(numFunctions);
    for (int i = 0; i < numFunctions; i++) {
      if (_nonScanResults != null && _nonScanResults[i] != null) {
        aggregationResults.add(_nonScanResults[i]);
      } else {
        aggregationResults.add(_aggregationFunctions[i].extractAggregationResult(_aggregationResultHolders[i]));
      }
    }
    return aggregationResults;
  }
}
