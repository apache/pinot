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
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


@SuppressWarnings({"rawtypes", "unchecked"})
public class DefaultAggregationExecutor implements AggregationExecutor {
  protected final AggregationFunction[] _aggregationFunctions;
  protected final AggregationResultHolder[] _aggregationResultHolders;

  public DefaultAggregationExecutor(AggregationFunction[] aggregationFunctions) {
    _aggregationFunctions = aggregationFunctions;
    int numAggregationFunctions = aggregationFunctions.length;
    _aggregationResultHolders = new AggregationResultHolder[numAggregationFunctions];
    for (int i = 0; i < numAggregationFunctions; i++) {
      _aggregationResultHolders[i] = aggregationFunctions[i].createAggregationResultHolder();
    }
  }

  @Override
  public void aggregate(ValueBlock valueBlock) {
    int numAggregationFunctions = _aggregationFunctions.length;
    int length = valueBlock.getNumDocs();
    for (int i = 0; i < numAggregationFunctions; i++) {
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
      aggregationResults.add(_aggregationFunctions[i].extractAggregationResult(_aggregationResultHolders[i]));
    }
    return aggregationResults;
  }
}
