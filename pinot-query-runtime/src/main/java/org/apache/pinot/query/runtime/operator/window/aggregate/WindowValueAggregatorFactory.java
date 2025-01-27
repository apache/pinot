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
package org.apache.pinot.query.runtime.operator.window.aggregate;

import org.apache.pinot.common.utils.DataSchema;


public class WindowValueAggregatorFactory {

  private WindowValueAggregatorFactory() {
  }

  /**
   * Returns an instance of a window value aggregator for the given window function name and column data type.
   *
   * @param functionName The window function name
   * @param columnDataType The column data type for the value being aggregated
   * @param supportRemoval Whether the window aggregator should support removal of values; some cases require only
   *                       addition of values and certain window functions can be more efficiently computed in these
   *                       cases
   * @return The window value aggregator instance
   */
  public static WindowValueAggregator<Object> getWindowValueAggregator(String functionName,
      DataSchema.ColumnDataType columnDataType, boolean supportRemoval) {
    // TODO: Add type specific aggregator implementations
    switch (functionName) {
      // NOTE: Keep both 'SUM0' and '$SUM0' for backward compatibility where 'SUM0' is SqlKind and '$SUM0' is function
      // name.
      case "SUM":
      case "SUM0":
      case "$SUM0":
        return new SumWindowValueAggregator();
      case "MIN":
        return new MinWindowValueAggregator(supportRemoval);
      case "MAX":
        return new MaxWindowValueAggregator(supportRemoval);
      case "COUNT":
        return new CountWindowValueAggregator();
      case "BOOLAND":
        return new BoolAndValueAggregator();
      case "BOOLOR":
        return new BoolOrValueAggregator();
      default:
        throw new IllegalArgumentException("Unsupported aggregate function: " + functionName);
    }
  }
}
