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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class DistinctAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new DataTypeScenario(FieldSpec.DataType.INT),
        new DataTypeScenario(FieldSpec.DataType.LONG),
        new DataTypeScenario(FieldSpec.DataType.FLOAT),
        new DataTypeScenario(FieldSpec.DataType.DOUBLE)
    };
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select count(distinct myField) from testTable")
        .thenResultIs("INTEGER", "1");
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select count(distinct myField) from testTable")
        .thenResultIs("INTEGER", "0");
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationGroupBySVAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select 'literal', count(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | INTEGER", "literal | 1");
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationGroupBySVAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select 'literal', count(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | INTEGER", "literal | 0");
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "2",
            "2"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select count(distinct myField) from testTable")
        .thenResultIs("INTEGER", "3");
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "2",
            "2"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select count(distinct myField) from testTable")
        .thenResultIs("INTEGER", "2");
  }

  @Test(dataProvider = "scenarios")
  void distinctCountAggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "3",
            "2"
        ).whenQuery("select 'literal', count(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | INTEGER", "literal | 3");
  }

  @Test(dataProvider = "scenarios")
  void distinctSumAggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "2",
            "2"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select sum(distinct myField) from testTable")
        .thenResultIs("DOUBLE", addToDefaultNullValue(scenario.getDataType(), 3));
  }

  @Test(dataProvider = "scenarios")
  void distinctSumAggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "2",
            "2"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select sum(distinct myField) from testTable")
        .thenResultIs("DOUBLE", "3");
  }

  @Test(dataProvider = "scenarios")
  void distinctSumAggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "3",
            "2"
        ).whenQuery("select 'literal', sum(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | " + addToDefaultNullValue(scenario.getDataType(), 6));
  }

  @Test(dataProvider = "scenarios")
  void distinctSumAggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "3",
            "2"
        ).whenQuery("select 'literal', sum(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 6");
  }

  @Test(dataProvider = "scenarios")
  void distinctAvgAggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "null",
            "null"
        ).whenQuery("select avg(distinct myField) from testTable")
        .thenResultIs("DOUBLE", "1.0");
  }

  @Test(dataProvider = "scenarios")
  void distinctAvgAggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "null",
            "null"
        ).whenQuery("select avg(distinct myField) from testTable")
        .thenResultIs("DOUBLE", "1.5");
  }

  @Test(dataProvider = "scenarios")
  void distinctAvgAggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "3",
            "2"
        ).whenQuery("select 'literal', avg(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 1.5");
  }

  @Test(dataProvider = "scenarios")
  void distinctAvgAggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "1",
            "2"
        ).andOnSecondInstance("myField",
            "2",
            "3",
            "2"
        ).whenQuery("select 'literal', avg(distinct myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 2.0");
  }

  private String addToDefaultNullValue(FieldSpec.DataType dataType, int addend) {
    switch (dataType) {
      case INT:
        return String.valueOf(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT + addend);
      case LONG:
        return String.valueOf(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG + addend);
      case FLOAT:
        return String.valueOf(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT + addend);
      case DOUBLE:
        return String.valueOf(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE + addend);
      default:
        throw new IllegalArgumentException("Unsupported data type: " + dataType);
    }
  }
}
