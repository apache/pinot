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


public class PercentileSmartTDigestAggregationFunctionTest {

  public static class WithHighThreshold extends AbstractPercentileAggregationFunctionTest {
    @Override
    public String callStr(String column, int percent) {
      return "PERCENTILESMARTTDIGEST(" + column + ", " + percent + ", 'THRESHOLD=10000')";
    }
  }

  public static class WithSmallThreshold extends AbstractPercentileAggregationFunctionTest {
    @Override
    public String callStr(String column, int percent) {
      return "PERCENTILESMARTTDIGEST(" + column + ", " + percent + ", 'THRESHOLD=1')";
    }

    // t-digest 3.3 changed interpolation for small datasets: values snap to integers
    // instead of interpolating between adjacent values (e.g., p10 returns 1.0 not 0.5)
    @Override
    String expectedAggrWithNull10(Scenario scenario) {
      return "1.0";
    }

    @Override
    String expectedAggrWithNull30(Scenario scenario) {
      return "3.0";
    }

    @Override
    String expectedAggrWithNull50(Scenario scenario) {
      return "5.0";
    }

    @Override
    String expectedAggrWithNull70(Scenario scenario) {
      return "7.0";
    }

    @Override
    String expectedAggrWithoutNull75(Scenario scenario) {
      return "4.0";
    }

    @Override
    String expectedAggrWithoutNull90(Scenario scenario) {
      return "7.0";
    }
  }
}
