/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.query.aggregation.function;

/**
 * No-op base class for aggregation function visitor
 */
// Keeping this base class (instead of interface) makes
// it easy to create multiple visitors overriding for specific methods
public class AggregationFunctionVisitorBase {

  public void visit(AggregationFunction function) {
    visitFunction(function);
  }

  public void visit(AvgAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(AvgMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(CountAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(CountMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(DistinctCountAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(DistinctCountHLLAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(DistinctCountHLLMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(DistinctCountMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(FastHLLAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(MaxAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(MaxMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(MinAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(MinMaxRangeAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(MinMaxRangeMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(MinMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(PercentileAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(PercentileMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(PercentileEstAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(PercentileEstMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(PercentileTDigestAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(SumAggregationFunction function) {
    visitFunction(function);
  }

  public void visit(SumMVAggregationFunction function) {
    visitFunction(function);
  }

  public void visitFunction(AggregationFunction function) {
    // non-op
  }
}

