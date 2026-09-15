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
import javax.annotation.Nullable;
import org.apache.datasketches.tuple.TupleSketch;
import org.apache.datasketches.tuple.TupleSketchIterator;
import org.apache.datasketches.tuple.aninteger.IntegerSummary;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.customobject.TupleIntSketchAccumulator;
import org.apache.pinot.segment.spi.AggregationFunctionType;


public class AvgValueIntegerTupleSketchAggregationFunction
    extends IntegerTupleSketchAggregationFunction {

  public AvgValueIntegerTupleSketchAggregationFunction(List<ExpressionContext> arguments, IntegerSummary.Mode mode,
      boolean nullHandlingEnabled) {
    super(arguments, mode, nullHandlingEnabled);
  }

  // TODO if extra aggregation modes are supported, make this switch
  // ie, if a Mode argument other than SUM is passed in, switch to the matching AggregationFunctionType
  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.AVGVALUEINTEGERSUMTUPLESKETCH;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG;
  }

  @Nullable
  @Override
  public Comparable extractFinalResult(@Nullable TupleIntSketchAccumulator accumulator) {
    // A null intermediate result means nothing was aggregated, and so does an empty sketch, which the retained-entry
    // check below already answers NULL for. The average of nothing is NULL in either mode.
    if (accumulator == null) {
      return null;
    }
    accumulator.setNominalEntries(_nominalEntries);
    accumulator.setSetOperations(_setOps);
    accumulator.setThreshold(_accumulatorThreshold);
    TupleSketch<IntegerSummary> result = accumulator.getResult();
    if (result.isEmpty() || result.getRetainedEntries() == 0) {
      // there is nothing to average, return null
      return null;
    }
    TupleSketchIterator<IntegerSummary> summaries = result.iterator();
    double retainedTotal = 0L;
    while (summaries.next()) {
      retainedTotal += summaries.getSummary().getValue();
    }
    double estimate = retainedTotal / result.getRetainedEntries();
    return Math.round(estimate);
  }
}
