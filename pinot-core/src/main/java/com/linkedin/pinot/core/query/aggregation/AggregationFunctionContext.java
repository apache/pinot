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
package com.linkedin.pinot.core.query.aggregation;

import com.linkedin.pinot.common.request.AggregationInfo;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunction;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionFactory;


/**
 * This class caches miscellaneous data to perform efficient aggregation.
 */
public class AggregationFunctionContext {
  private final AggregationFunction _aggregationFunction;
  private final String[] _aggrColumns;

  public static AggregationFunctionContext instantiate(AggregationInfo aggregationInfo) {
    String[] aggrColumns = aggregationInfo.getAggregationParams().get("column").trim().split(",");
    String functionName = aggregationInfo.getAggregationType();
    AggregationFunction aggregationFunction = AggregationFunctionFactory.getAggregationFunction(functionName);
    return new AggregationFunctionContext(aggrColumns, aggregationFunction);
  }

  public AggregationFunctionContext(String[] aggrColumns, AggregationFunction aggregationFunction) {
    _aggrColumns = aggrColumns;
    _aggregationFunction = aggregationFunction;
  }

  /**
   * Returns the aggregation function object.
   * @return
   */
  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * Returns an array of aggregation column names.
   * @return
   */
  public String[] getAggregationColumns() {
    return _aggrColumns;
  }

  /**
   * Returns the aggregation column name for the results.
   */
  public String getAggregationColumnName() {
    return _aggregationFunction.getColumnName(_aggrColumns);
  }
}
