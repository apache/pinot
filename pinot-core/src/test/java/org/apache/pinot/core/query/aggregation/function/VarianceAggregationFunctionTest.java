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

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.core.query.aggregation.utils.StatisticalAggregationFunctionUtils.calculateVariance;


public class VarianceAggregationFunctionTest extends AbstractAggregationFunctionTest {

  private static final EnumSet<AggregationFunctionType> VARIANCE_FUNCTIONS = EnumSet.of(AggregationFunctionType.VARPOP,
      AggregationFunctionType.VARSAMP, AggregationFunctionType.STDDEVPOP, AggregationFunctionType.STDDEVSAMP);

  private static final Set<FieldSpec.DataType> DATA_TYPES = Set.of(FieldSpec.DataType.INT, FieldSpec.DataType.LONG,
      FieldSpec.DataType.FLOAT, FieldSpec.DataType.DOUBLE);

  @DataProvider(name = "scenarios")
  Object[][] scenarios() {
    Object[][] scenarios = new Object[16][2];

    int i = 0;
    for (AggregationFunctionType functionType : VARIANCE_FUNCTIONS) {
      for (FieldSpec.DataType dataType : DATA_TYPES) {
        scenarios[i][0] = functionType;
        scenarios[i][1] = new DataTypeScenario(dataType);
        i++;
      }
    }

    return scenarios;
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingDisabled(AggregationFunctionType functionType, DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select " + functionType.getName() + "(myField) from testTable")
        .thenResultIs("DOUBLE", "0.0");
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingEnabled(AggregationFunctionType functionType, DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select " + functionType.getName() + "(myField) from testTable")
        .thenResultIs("DOUBLE", "null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled(AggregationFunctionType functionType,
      DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', " + functionType.getName() + "(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 0.0");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled(AggregationFunctionType functionType,
      DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', " + functionType.getName() + "(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingDisabled(AggregationFunctionType functionType, DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "3",
            "6",
            "null"
        ).whenQuery("select " + functionType.getName() + "(myField) from testTable")
        .thenResultIs("DOUBLE", String.valueOf(calculateVariance(List.of(1.0, 0.0, 2.0, 3.0, 6.0, 0.0),
            functionType)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingEnabled(AggregationFunctionType functionType, DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "3",
            "6",
            "null"
        ).whenQuery("select " + functionType.getName() + "(myField) from testTable")
        .thenResultIs("DOUBLE", String.valueOf(calculateVariance(List.of(1.0, 2.0, 3.0, 6.0), functionType)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingDisabled(AggregationFunctionType functionType, DataTypeScenario scenario) {
    scenario.getDeclaringTable(false, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "3",
            "6",
            "null"
        ).whenQuery("select 'literal', " + functionType.getName() + "(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | "
            + calculateVariance(List.of(1.0, 0.0, 2.0, 3.0, 6.0, 0.0), functionType));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingEnabled(AggregationFunctionType functionType, DataTypeScenario scenario) {
    scenario.getDeclaringTable(true, FieldSpec.FieldType.METRIC)
        .onFirstInstance("myField",
            "1",
            "null",
            "2"
        ).andOnSecondInstance("myField",
            "3",
            "6",
            "null"
        ).whenQuery("select 'literal', " + functionType.getName() + "(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | "
            + calculateVariance(List.of(1.0, 2.0, 3.0, 6.0), functionType));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupByMV(AggregationFunctionType functionType, DataTypeScenario scenario) {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .addMetricField("value", scenario.getDataType())
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", 1},
            new Object[]{"tag1;tag2", null},
            new Object[]{"tag1;tag2", 2}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 3},
            new Object[]{"tag2;tag1", 6},
            new Object[]{"tag1;tag2", null}
        )
        .whenQuery("select tags, " + functionType.getName() + "(value) from testTable group by tags order by tags")
        .thenResultIs(new Object[]{"tag1", calculateVariance(List.of(1.0, 0.0, 2.0, 3.0, 6.0, 0.0), functionType)},
            new Object[]{"tag2", calculateVariance(List.of(1.0, 0.0, 2.0, 3.0, 6.0, 0.0), functionType)})
        .whenQueryWithNullHandlingEnabled("select tags, " + functionType.getName() + "(value) from testTable "
            + "group by tags order by tags")
        .thenResultIs(new Object[]{"tag1", calculateVariance(List.of(1.0, 2.0, 3.0, 6.0), functionType)},
            new Object[]{"tag2", calculateVariance(List.of(1.0, 2.0, 3.0, 6.0), functionType)});
  }
}
