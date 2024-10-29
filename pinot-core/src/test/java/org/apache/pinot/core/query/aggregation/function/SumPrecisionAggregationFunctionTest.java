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

import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class SumPrecisionAggregationFunctionTest extends AbstractAggregationFunctionTest {

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
        ).whenQuery("select sumprecision(myField) from testTable")
        .thenResultIs("STRING",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.METRIC, scenario.getDataType(), null)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select sumprecision(myField) from testTable")
        .thenResultIs("STRING", "null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', sumprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | "
            + FieldSpec.getDefaultNullValue(FieldSpec.FieldType.METRIC, scenario.getDataType(), null));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', sumprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "3",
            "null",
            "5"
        ).andOnSecondInstance("myField",
            "null",
            "null"
        ).whenQuery("select sumprecision(myField) from testTable")
        .thenResultIs("STRING", getStringValueOfSum(8, scenario.getDataType()));
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "5",
            "null"
        ).andOnSecondInstance("myField",
            "2",
            "null",
            "3"
        ).whenQuery("select sumprecision(myField) from testTable")
        .thenResultIs("STRING", getStringValueOfSum(10, scenario.getDataType()));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "2",
            "null"
        ).whenQuery("select 'literal', sumprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | " + getStringValueOfSum(10, scenario.getDataType()));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select 'literal', sumprecision(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | " + getStringValueOfSum(8, scenario.getDataType()));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupByMV(DataTypeScenario scenario) {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .addSingleValueDimension("value", scenario.getDataType(), -1)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", 1},
            new Object[]{"tag2;tag3", null}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 2},
            new Object[]{"tag2;tag3", null}
        )
        .whenQuery("select tags, sumprecision(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | STRING",
            "tag1    | " + getStringValueOfSum(3, scenario.getDataType()),
            "tag2    | " + getStringValueOfSum(1, scenario.getDataType()),
            "tag3    | " + getStringValueOfSum(-2, scenario.getDataType())
        )
        .whenQueryWithNullHandlingEnabled("select tags, sumprecision(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | STRING",
            "tag1    | " + getStringValueOfSum(3, scenario.getDataType()),
            "tag2    | " + getStringValueOfSum(3, scenario.getDataType()),
            "tag3    | null"
        );
  }

  private String getStringValueOfSum(int sum, FieldSpec.DataType dataType) {
    if (dataType == FieldSpec.DataType.FLOAT || dataType == FieldSpec.DataType.DOUBLE) {
      return String.valueOf((double) sum);
    } else {
      return String.valueOf(sum);
    }
  }
}
