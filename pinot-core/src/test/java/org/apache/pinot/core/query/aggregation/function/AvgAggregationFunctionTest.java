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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec.PASS_THROUGH;


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
        .whenQuery("select tags, AVG(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1    | 1.5",
            "tag2    | 0.75",
            "tag3    | 0.0"
        )
        .whenQueryWithNullHandlingEnabled("select tags, AVG(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1    | 1.5",
            "tag2    | 1.5",
            "tag3    | null"
        );
  }

  @Test(dataProvider = "encodingTypes")
  void singleKeyAggregationWithSmallNumGroupsLimitDoesntThrowAIOOBE(FieldConfig.EncodingType encoding) {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMetricField("key", FieldSpec.DataType.INT)
                .addMetricField("value", FieldSpec.DataType.INT)
                .build(),
            new TableConfigBuilder(TableType.OFFLINE)
                .setTableName("testTable")
                .addFieldConfig(
                    new FieldConfig("key", encoding, (FieldConfig.IndexType) null, PASS_THROUGH, null))
                .build())
        .onFirstInstance(new Object[]{7, 1}, new Object[]{6, 2}, new Object[]{5, 3}, new Object[]{4, 4})
        .andOnSecondInstance(new Object[]{7, 1}, new Object[]{6, 2}, new Object[]{5, 3}, new Object[]{4, 4})
        .whenQuery(
            "set numGroupsLimit=3; set maxInitialResultHolderCapacity=1000; "
                + "select key, avg(value) "
                + "from testTable "
                + "group by key "
                + "order by key")
        .thenResultIs(
            "INTEGER | DOUBLE",
            "5   |  3",
            "6   |  2",
            "7   |  1"
        );
  }

  @Test(dataProvider = "encodingTypes")
  void multiKeyAggregationWithSmallNumGroupsLimitDoesntThrowAIOOBE(FieldConfig.EncodingType encoding) {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMetricField("key1", FieldSpec.DataType.INT)
                .addMetricField("key2", FieldSpec.DataType.INT)
                .addMetricField("value", FieldSpec.DataType.INT)
                .build(),
            new TableConfigBuilder(TableType.OFFLINE)
                .setTableName("testTable")
                .addFieldConfig(
                    new FieldConfig("key1", encoding, (FieldConfig.IndexType) null, PASS_THROUGH, null))
                .addFieldConfig(
                    new FieldConfig("key2", encoding, (FieldConfig.IndexType) null, PASS_THROUGH, null))
                .build())
        .onFirstInstance(new Object[]{7, 1}, new Object[]{6, 2}, new Object[]{5, 3}, new Object[]{4, 4})
        .andOnSecondInstance(new Object[]{7, 1}, new Object[]{6, 2}, new Object[]{5, 3}, new Object[]{4, 4})
        .whenQuery(
            "set numGroupsLimit=3; set maxInitialResultHolderCapacity=1000; "
                + "select key1, key2, count(*) "
                + "from testTable "
                + "group by key1, key2 "
                + "order by key1, key2")
        .thenResultIs(
            "INTEGER | INTEGER | LONG",
            "5   |  3  |  2",
            "6   |  2  |  2",
            "7   |  1  |  2"
        );
  }

  @DataProvider(name = "encodingTypes")
  FieldConfig.EncodingType[] encodingTypes() {
    return FieldConfig.EncodingType.values();
  }
}
