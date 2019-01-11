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
package org.apache.pinot.tools.scan.query;

public class AggregationFuncFactory {
  private static final String COUNT = "COUNT";
  private static final String MIN = "MIN";
  private static final String MAX = "MAX";
  private static final String SUM = "SUM";
  private static final String AVG = "AVG";
  private static final String MINMAXRANGE = "MINMAXRANGE";
  private static final String DISTINCT_COUNT = "DISTINCTCOUNT";

  public static AggregationFunc getAggregationFunc(ResultTable rows, String column, String functionName) {
    switch (functionName.toUpperCase()) {
      case COUNT:
        return new CountFunction(rows, column);

      case MIN:
        return new MinFunction(rows, column);

      case MAX:
        return new MaxFunction(rows, column);

      case SUM:
        return new SumFunction(rows, column);

      case AVG:
        return new AvgFunction(rows, column);

      case MINMAXRANGE:
        return new MinMaxRangeFunction(rows, column);

      case DISTINCT_COUNT:
        return new DistinctCountFunction(rows, column);

      default:
        throw new RuntimeException("Unsupported aggregation function " + functionName);
    }
  }
}
