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

import java.util.Base64;
import java.util.List;
import org.apache.datasketches.theta.Sketch;
import org.apache.pinot.common.function.AggregationFunctionType;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.ExpressionContext;


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
  public String extractFinalResult(List<Sketch> sketches) {
    Sketch sketch = evaluatePostAggregationExpression(sketches);

    // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
    //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
    return Base64.getEncoder().encodeToString(sketch.compact(false, null).toByteArray());
  }
}
