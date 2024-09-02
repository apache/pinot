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


public class AvgAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new DataTypeScenario(FieldSpec.DataType.INT),
        new DataTypeScenario(FieldSpec.DataType.LONG),
        new DataTypeScenario(FieldSpec.DataType.FLOAT),
        new DataTypeScenario(FieldSpec.DataType.DOUBLE),
        new DataTypeScenario(FieldSpec.DataType.BIG_DECIMAL)
    };
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select avg(myField) from testTable")
        .thenResultIs("DOUBLE",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.METRIC, scenario.getDataType(), null)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select avg(myField) from testTable")
        .thenResultIs("DOUBLE", "null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', avg(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | "
            + FieldSpec.getDefaultNullValue(FieldSpec.FieldType.METRIC, scenario.getDataType(), null));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', avg(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "7",
            "null",
            "5"
        ).andOnSecondInstance("myField",
            "null",
            "3"
        ).whenQuery("select avg(myField) from testTable")
        .thenResultIs("DOUBLE", "3");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "7",
            "null",
            "5"
        ).andOnSecondInstance("myField",
            "null",
            "3"
        ).whenQuery("select avg(myField) from testTable")
        .thenResultIs("DOUBLE", "5");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "7",
            "null",
            "5"
        ).andOnSecondInstance("myField",
            "null",
            "3"
        ).whenQuery("select 'literal', avg(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 3");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "7",
            "null",
            "5"
        ).andOnSecondInstance("myField",
            "null",
            "3"
        ).whenQuery("select 'literal', avg(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 5");
  }
}
