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


public class MinLongAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @DataProvider(name = "scenarios")
  Object[] scenarios() {
    return new Object[] {
        new DataTypeScenario(FieldSpec.DataType.INT),
        new DataTypeScenario(FieldSpec.DataType.LONG)
    };
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select minlong(myField) from testTable")
        .thenResultIs("LONG",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, scenario.getDataType(), null)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select minlong(myField) from testTable")
        .thenResultIs("LONG", "null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', minlong(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | LONG", "literal | "
            + FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, scenario.getDataType(), null));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', minlong(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | LONG", "literal | null");
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "2",
            "null"
        ).whenQuery("select minlong(myField) from testTable")
        .thenResultIs("LONG",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, scenario.getDataType(), null)));
  }

  @Test(dataProvider = "scenarios")
  void aggregationWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "2",
            "null"
        ).whenQuery("select minlong(myField) from testTable")
        .thenResultIs("LONG", "2");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingDisabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(false)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "2",
            "null"
        ).whenQuery("select 'literal', minlong(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | LONG", "literal | "
            + FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, scenario.getDataType(), null));
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupBySVWithNullHandlingEnabled(DataTypeScenario scenario) {
    scenario.getDeclaringTable(true)
        .onFirstInstance("myField",
            "5",
            "null",
            "3"
        ).andOnSecondInstance("myField",
            "null",
            "null",
            "null"
        ).whenQuery("select 'literal', minlong(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | LONG", "literal | 3");
  }

  @Test(dataProvider = "scenarios")
  void aggregationGroupByMV(DataTypeScenario scenario) {
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
            new Object[]{"tag2;tag3", null}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 2},
            new Object[]{"tag2;tag3", null}
        )
        .whenQuery("select tags, minlong(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | LONG",
            "tag1    | 1",
            "tag2    | 0",
            "tag3    | 0"
        )
        .whenQueryWithNullHandlingEnabled("select tags, minlong(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | LONG",
            "tag1    | 1",
            "tag2    | 1",
            "tag3    | null"
        );
  }

  @Test
  public void aggregationOnLargeLongValues() {
    new DataTypeScenario(FieldSpec.DataType.LONG).getDeclaringTable(true)
        .onFirstInstance("myField",
            "" + Long.MAX_VALUE,
            "" + (Long.MAX_VALUE - 1),
            "" + (Long.MAX_VALUE - 2)
        ).andOnSecondInstance("myField",
            "" + (Long.MAX_VALUE - 3),
            "" + (Long.MAX_VALUE - 4),
            "" + (Long.MAX_VALUE - 5)
        ).whenQuery("select minlong(myField) from testTable")
        .thenResultIs("LONG", "" + (Long.MAX_VALUE - 5));
  }

  @Test
  public void aggregationMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.LONG)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3"}
        )
        .andOnSecondInstance(
            new Object[]{"null"}
        )
        .whenQuery("select minlong(mv) from testTable")
        .thenResultIs("LONG",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.LONG, null)))
        .whenQueryWithNullHandlingEnabled("select minlong(mv) from testTable")
        .thenResultIs("LONG", "1");
  }

  @Test
  public void aggregationMVGroupBySV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.LONG)
                .addSingleValueDimension("sv", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"null", "k1"},
            new Object[]{"1;2;3", "k2"}
        )
        .andOnSecondInstance(
            new Object[]{"null", "k2"},
            new Object[]{"1;2;3", "k1"}
        )
        .whenQuery("select minlong(mv) from testTable group by sv")
        .thenResultIs("LONG", String.valueOf(
            ((Number) FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.LONG,
                null)).longValue()), String.valueOf(
            ((Number) FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.LONG,
                null)).longValue()))
        .whenQueryWithNullHandlingEnabled("select minlong(mv) from testTable group by sv")
        .thenResultIs("LONG", "1", "1");
  }

  @Test
  public void aggregationMVGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv1", FieldSpec.DataType.LONG)
                .addMultiValueDimension("mv2", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2", "k1;k2"}
        )
        .andOnSecondInstance(
            new Object[]{"null", "k1;k2"}
        )
        .whenQuery("select minlong(mv1) from testTable group by mv2")
        .thenResultIs("LONG",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.LONG, null)),
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.LONG, null)))
        .whenQueryWithNullHandlingEnabled("select minlong(mv1) from testTable group by mv2")
        .thenResultIs("LONG", "1", "1");
  }
}
