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

    @Override
    String expectedAggrWithNull10(Scenario scenario) {
      return "0.5";
    }

    @Override
    String expectedAggrWithNull30(Scenario scenario) {
      return "2.5";
    }

    @Override
    String expectedAggrWithNull50(Scenario scenario) {
      return "4.5";
    }

    @Override
    String expectedAggrWithNull70(Scenario scenario) {
      return "6.5";
    }

    @Override
    String expectedAggrWithoutNull55(Scenario scenario) {
      switch (scenario.getDataType()) {
        case INT:
          return "-6.442450943999939E8";
        case LONG:
          return "-2.7670116110564065E18";
        case FLOAT:
        case DOUBLE:
          return "-Infinity";
        default:
          throw new IllegalArgumentException("Unsupported datatype " + scenario.getDataType());
      }
    }

    @Override
    String expectedAggrWithoutNull75(Scenario scenario) {
      return "4.0";
    }

    @Override
    String expectedAggrWithoutNull90(Scenario scenario) {
      return "7.100000000000001";
    }

    @Override
    String expectedAggrWithoutNull100(Scenario scenario) {
      return super.expectedAggrWithoutNull100(scenario);
    }
  }
}
