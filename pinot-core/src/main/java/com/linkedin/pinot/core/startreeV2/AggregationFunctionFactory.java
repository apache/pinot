/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;

public class AggregationFunctionFactory {

  private static SumAggregationFunction _sumFunction;
  private static MinAggregationFunction _minFunction;
  private static MaxAggregationFunction _maxFunction;
  private static CountAggregationFunction _countFunction;
  private static DistinctCountHLLAggregationFunction _distinctCountHLLFunction;
  private static PercentileTDigestAggregationFunction _percentileTDigestFunction;

  public AggregationFunctionFactory() {
    _sumFunction = new SumAggregationFunction();
    _minFunction = new MinAggregationFunction();
    _maxFunction = new MaxAggregationFunction();
    _countFunction = new CountAggregationFunction();
    _distinctCountHLLFunction = new DistinctCountHLLAggregationFunction();
    _percentileTDigestFunction = new PercentileTDigestAggregationFunction();
  }

  public static AggregationFunction getAggregationFunction(String functionName) {

    switch (functionName) {
      case StarTreeV2Constant.AggregateFunctions.SUM:
        return _sumFunction;
      case StarTreeV2Constant.AggregateFunctions.MIN:
        return _minFunction;
      case StarTreeV2Constant.AggregateFunctions.MAX:
        return _maxFunction;
      case StarTreeV2Constant.AggregateFunctions.COUNT:
        return _countFunction;
      case StarTreeV2Constant.AggregateFunctions.DISTINCTCOUNTHLL:
        return _distinctCountHLLFunction;
      case StarTreeV2Constant.AggregateFunctions.PERCENTILETDIGEST:
        return _percentileTDigestFunction;
      default:
        throw new UnsupportedOperationException();
    }
  }
}
