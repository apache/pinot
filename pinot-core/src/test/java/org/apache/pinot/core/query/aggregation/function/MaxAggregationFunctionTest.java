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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.FieldSpec.FieldType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MaxAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new DataTypeScenario(DataType.INT),
        new DataTypeScenario(DataType.LONG),
        new DataTypeScenario(DataType.FLOAT),
        new DataTypeScenario(DataType.DOUBLE),
        new DataTypeScenario(DataType.BIG_DECIMAL)
    };
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null")
        .whenQuery("select max(myField) from testTable")
        .thenResultIs("DOUBLE",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldType.DIMENSION, scenario.getDataType(), null)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null")
        .whenQuery("select max(myField) from testTable")
        .thenResultIs("DOUBLE", "null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null")
        .whenQuery("select 'literal', max(myField) from testTable group by 'literal'")
        .thenResultIs(
            "STRING | DOUBLE",
            "literal | " + FieldSpec.getDefaultNullValue(FieldType.DIMENSION, scenario.getDataType(), null)
        );
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "null", "null")
        .andOnSecondInstance("myField", "null")
        .whenQuery("select 'literal', max(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "2", "null", "3")
        .andOnSecondInstance("myField", "null", "5", "null")
        .whenQuery("select max(myField) from testTable")
        .thenResultIs("DOUBLE", "5");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "2", "null", "3")
        .andOnSecondInstance("myField", "null", "5", "null")
        .whenQuery("select max(myField) from testTable")
        .thenResultIs("DOUBLE", "5");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField", "2", "null", "3")
        .andOnSecondInstance("myField", "null", "5", "null")
        .whenQuery("select 'literal', max(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 5");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField", "3", "null", "5")
        .andOnSecondInstance("myField", "null", "null", "null")
        .whenQuery("select 'literal', max(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | DOUBLE", "literal | 5");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupByMV(DataTypeScenario scenario) {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("tags", DataType.STRING)
        .addMetricField("value", scenario.getDataType())
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"tag1;tag2", -1}, new Object[]{"tag2;tag3", null})
        .andOnSecondInstance(new Object[]{"tag1;tag2", -2}, new Object[]{"tag2;tag3", null})
        .whenQuery("select tags, MAX(value) from testTable group by tags order by tags")
        .thenResultIs("STRING | DOUBLE", "tag1    | -1.0", "tag2    | 0.0", "tag3    | 0.0")
        .whenQueryWithNullHandlingEnabled("select tags, MAX(value) from testTable group by tags order by tags")
        .thenResultIs("STRING | DOUBLE", "tag1    | -1.0", "tag2    | -1.0", "tag3    | null");
  }

  @Test
  public void aggregationMVAllNulls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv", DataType.INT)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"null"})
        .andOnSecondInstance(new Object[]{"null"})
        .whenQuery("select max(mv) from testTable")
        .thenResultIs(
            "DOUBLE",
            String.valueOf((int) FieldSpec.getDefaultNullValue(FieldType.DIMENSION, DataType.INT, null))
        )
        .whenQueryWithNullHandlingEnabled("select max(mv) from testTable")
        .thenResultIs("DOUBLE", "null");
  }

  @Test
  public void aggregationMVWithNulls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv", DataType.INT)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"1;2;3"})
        .andOnSecondInstance(new Object[]{"null"})
        .whenQuery("select max(mv) from testTable")
        .thenResultIs("DOUBLE", "3")
        .whenQueryWithNullHandlingEnabled("select max(mv) from testTable")
        .thenResultIs("DOUBLE", "3");
  }

  @Test
  public void aggregationMVGroupBySVAllNulls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv", DataType.INT)
        .addSingleValueDimension("sv", DataType.STRING)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"null", "k1"})
        .andOnSecondInstance(new Object[]{"null", "k1"})
        .whenQuery("select max(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", String.valueOf(FieldSpec.getDefaultNullValue(FieldType.DIMENSION, DataType.INT, null)))
        .whenQueryWithNullHandlingEnabled("select max(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "null");
  }

  @Test
  public void aggregationGroupBySVWithNulls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv", DataType.INT)
        .addSingleValueDimension("sv", DataType.STRING)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"null", "k1"}, new Object[]{"1;2;3", "k2"})
        .andOnSecondInstance(new Object[]{"null", "k2"}, new Object[]{"1;2;3", "k1"})
        .whenQuery("select max(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "3", "3")
        .whenQueryWithNullHandlingEnabled("select max(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "3", "3");
  }

  @Test
  public void aggregationMVGroupByMVAllNulls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv1", DataType.INT)
        .addMultiValueDimension("mv2", DataType.STRING)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"null", "k1;k2"})
        .andOnSecondInstance(new Object[]{"null", "k1;k2"})
        .whenQuery("select max(mv1) from testTable group by mv2")
        .thenResultIs(
            "DOUBLE",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldType.DIMENSION, DataType.INT, null)),
            String.valueOf(FieldSpec.getDefaultNullValue(FieldType.DIMENSION, DataType.INT, null))
        )
        .whenQueryWithNullHandlingEnabled("select max(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "null", "null");
  }

  @Test
  public void aggregationMVGroupByMVWithNulls() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv1", DataType.INT)
        .addMultiValueDimension("mv2", DataType.STRING)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"1;2", "k1;k2"})
        .andOnSecondInstance(new Object[]{"null", "k1;k2"})
        .whenQuery("select max(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "2", "2")
        .whenQueryWithNullHandlingEnabled("select max(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "2", "2");
  }
}
