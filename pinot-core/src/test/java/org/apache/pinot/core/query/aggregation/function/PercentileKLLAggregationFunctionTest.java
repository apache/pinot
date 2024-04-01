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


public class PercentileKLLAggregationFunctionTest extends AbstractPercentileAggregationFunctionTest {
  @Override
  public String callStr(String column, int percent) {
    return "PERCENTILEKLL(" + column + ", " + percent + ")";
  }

  @Override
  String expectedAggrWithNull10(Scenario scenario) {
    return "0";
  }

  @Override
  String expectedAggrWithNull30(Scenario scenario) {
    return "2";
  }

  @Override
  String expectedAggrWithNull50(Scenario scenario) {
    return "4";
  }

  @Override
  String expectedAggrWithNull70(Scenario scenario) {
    return "6";
  }
}
