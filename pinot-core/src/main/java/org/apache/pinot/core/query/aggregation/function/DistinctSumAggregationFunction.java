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

import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.spi.AggregationFunctionType;

/**
 * Aggregation function to compute the sum of distinct values.
 */
public class DistinctSumAggregationFunction extends BaseDistinctAggregateAggregationFunction<Double> {

  public DistinctSumAggregationFunction(ExpressionContext expression) {
    super(expression, AggregationFunctionType.DISTINCTSUM);
  }

  @Override
  public DataSchema.ColumnDataType getFinalResultColumnType() {
    return DataSchema.ColumnDataType.DOUBLE;
  }

  @Override
  public Double extractFinalResult(Set intermediateResult) {
    Double distinctSum = 0.0;

    for (Object obj : intermediateResult) {
      distinctSum += ((Number) obj).doubleValue();
    }

    return distinctSum;
  }
}
