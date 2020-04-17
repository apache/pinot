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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.AggregationFunctionUtils;


/**
 * This class caches miscellaneous data to perform efficient aggregation.
 *
 * TODO: Remove this class, as it no longer provides any value after aggregation functions now store
 * their arguments.
 */
public class AggregationFunctionContext {
  private final AggregationFunction _aggregationFunction;
  private final List<String> _expressions;
  private final String columnName;

  public AggregationFunctionContext(AggregationFunction aggregationFunction, List<String> expressions) {
    Preconditions.checkArgument(expressions.size() >= 1, "Aggregation functions require at least one argument.");
    _aggregationFunction = aggregationFunction;
    _expressions = expressions;
    columnName = AggregationFunctionUtils.concatArgs(expressions);
  }

  /**
   * Returns the aggregation function.
   */
  public AggregationFunction getAggregationFunction() {
    return _aggregationFunction;
  }

  /**
   * Returns the arguments for the aggregation function.
   *
   * @return List of Strings containing the arguments for the aggregation function.
   */
  public List<String> getExpressions() {
    return _expressions;
  }

  /**
   * Returns the column for aggregation function.
   *
   * @return Aggregation Column (could be column name or UDF expression).
   */
  public String getColumnName() {
    return columnName;
  }

  /**
   * Returns the aggregation column name for the results.
   * <p>E.g. AVG(foo) -> avg_foo
   */
  public String getAggregationColumnName() {
    return _aggregationFunction.getColumnName();
  }

  /**
   * Returns the aggregation column name for the result table.
   * <p>E.g. AVGMV(foo) -> avgMV(foo)
   */
  public String getResultColumnName() {
    return _aggregationFunction.getResultColumnName();
  }
}
