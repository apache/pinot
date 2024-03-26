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
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.customobject.CpcSketchAccumulator;
import org.apache.pinot.segment.local.customobject.SerializedCPCSketch;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * The {@code DistinctCountRawCPCAggregationFunction} shares the same usage as the
 * {@link DistinctCountCPCSketchAggregationFunction}, and returns the sketch as a base64 encoded string.
 */
public class DistinctCountRawCPCSketchAggregationFunction extends DistinctCountCPCSketchAggregationFunction {

  public DistinctCountRawCPCSketchAggregationFunction(List<ExpressionContext> arguments) {
    super(arguments);
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.DISTINCTCOUNTRAWCPCSKETCH;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.STRING;
  }

  @Override
  public SerializedCPCSketch extractFinalResult(CpcSketchAccumulator intermediateResult) {
    intermediateResult.setLgNominalEntries(_lgNominalEntries);
    intermediateResult.setThreshold(_accumulatorThreshold);
    return new SerializedCPCSketch(intermediateResult.getResult());
  }
}
