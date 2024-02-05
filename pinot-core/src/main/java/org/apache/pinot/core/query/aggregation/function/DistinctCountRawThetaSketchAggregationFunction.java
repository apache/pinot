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

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.apache.datasketches.theta.Sketch;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.customobject.ThetaSketchAccumulator;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The {@code DistinctCountRawThetaSketchAggregationFunction} shares the same usage as the
 * {@link DistinctCountThetaSketchAggregationFunction}, and returns the sketch as a base64 encoded string.
 */
public class DistinctCountRawThetaSketchAggregationFunction extends DistinctCountThetaSketchAggregationFunction {

  public DistinctCountRawThetaSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWTHETASKETCH;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public String extractFinalResult(List<ThetaSketchAccumulator> accumulators) {
    int numAccumulators = accumulators.size();
    List<Sketch> mergedSketches = new ArrayList<>(numAccumulators);

    for (Object object : accumulators) {
      ThetaSketchAccumulator accumulator = convertSketchAccumulator(object);
      accumulator.setThreshold(_accumulatorThreshold);
      accumulator.setSetOperationBuilder(_setOperationBuilder);
      mergedSketches.add(accumulator.getResult());
    }

    Sketch sketch = evaluatePostAggregationExpression(mergedSketches);
    return Base64.getEncoder().encodeToString(sketch.toByteArray());
  }
}
