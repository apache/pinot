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

import org.apache.pinot.spi.data.FieldSpec;


public class PercentileEstAggregationFunctionTest extends AbstractPercentileAggregationFunctionTest {
  @Override
  public String callStr(String column, int percent) {
    return "PERCENTILEEST(" + column + ", " + percent + ")";
  }

  @Override
  public String getFinalResultColumnType() {
    return "LONG";
  }

  String minValue(FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT: return "-2147483648";
      case LONG: return "-9223372036854775808";
      case FLOAT: return "-9223372036854775808";
      case DOUBLE: return "-9223372036854775808";
      default:
        throw new IllegalArgumentException("Unexpected type " + dataType);
    }
  }
}
