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

/**
 * No-op base class for aggregation function visitor.
 */
@SuppressWarnings("unused")
public class AggregationFunctionVisitorBase {

  public void visit(AvgAggregationFunction function) {
  }

  public void visit(AvgMVAggregationFunction function) {
  }

  public void visit(CountAggregationFunction function) {
  }

  public void visit(CountMVAggregationFunction function) {
  }

  public void visit(DistinctAggregationFunction function) {
  }

  public void visit(DistinctCountAggregationFunction function) {
  }

  public void visit(DistinctCountMVAggregationFunction function) {
  }

  public void visit(DistinctCountBitmapAggregationFunction function) {
  }

  public void visit(DistinctCountBitmapMVAggregationFunction function) {
  }

  public void visit(DistinctCountHLLAggregationFunction function) {
  }

  public void visit(DistinctCountHLLMVAggregationFunction function) {
  }

  public void visit(FastHLLAggregationFunction function) {
  }

  public void visit(MaxAggregationFunction function) {
  }

  public void visit(MaxMVAggregationFunction function) {
  }

  public void visit(MinAggregationFunction function) {
  }

  public void visit(MinMVAggregationFunction function) {
  }

  public void visit(MinMaxRangeAggregationFunction function) {
  }

  public void visit(MinMaxRangeMVAggregationFunction function) {
  }

  public void visit(PercentileAggregationFunction function) {
  }

  public void visit(PercentileMVAggregationFunction function) {
  }

  public void visit(PercentileEstAggregationFunction function) {
  }

  public void visit(PercentileEstMVAggregationFunction function) {
  }

  public void visit(PercentileTDigestAggregationFunction function) {
  }

  public void visit(PercentileTDigestMVAggregationFunction function) {
  }

  public void visit(SumAggregationFunction function) {
  }

  public void visit(SumMVAggregationFunction function) {
  }

  public void visit(DistinctCountThetaSketchAggregationFunction function) {
  }

  public void visit(StUnionAggregationFunction function) {

  }
}

