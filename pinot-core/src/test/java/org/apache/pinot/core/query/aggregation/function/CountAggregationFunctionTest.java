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


public class CountAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void list() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[] {1}
        )
        .andOnSecondInstance(
            new Object[] {2},
            new Object[] {null}
        )
        .whenQuery("select myField from testTable order by myField")
        .thenResultIs("INTEGER",
            "-2147483648",
            "1",
            "2"
        );
  }

  @Test
  public void listNullHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[] {1}
        )
        .andOnSecondInstance(
            new Object[] {2},
            new Object[] {null}
        )
        .whenQuery("select myField from testTable order by myField")
        .thenResultIs("INTEGER",
            "1",
            "2",
            "null"
        );
  }

  @Test
  public void countNullWhenHandlingDisabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1"
        )
        .andOnSecondInstance(
            "myField",
            "2",
            "null"
        )
        .whenQuery("select myField, COUNT(myField) from testTable group by myField order by myField")
        .thenResultIs("INTEGER | LONG",
            "-2147483648 | 1",
            "1           | 1",
            "2           | 1"
        );
  }


  @Test
  public void countNullWhenHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1"
        )
        .andOnSecondInstance(
            "myField",
            "2",
            "null"
        )
        .whenQuery("select myField, COUNT(myField) from testTable group by myField order by myField")
        .thenResultIs(
            "INTEGER | LONG",
            "1    | 1",
            "2    | 1",
            "null | 0"
        );
  }

  @Test
  public void countStarNullWhenHandlingDisabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1"
        )
        .andOnSecondInstance(
            "myField",
            "2",
            "null"
        )
        .whenQuery("select myField, COUNT(*) from testTable group by myField order by myField")
        .thenResultIs("INTEGER | LONG",
            "-2147483648 | 1",
            "1    | 1",
            "2    | 1"
        );
  }

  @Test
  public void countStarNullWhenHandlingEnabled() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(true)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1"
        )
        .andOnSecondInstance(
            "myField",
            "2",
            "null"
        )
        .whenQuery("select myField, COUNT(*) from testTable group by myField order by myField")
        .thenResultIs("INTEGER | LONG",
            "1    | 1",
            "2    | 1",
            "null | 1"
        );
  }

  @Test(dataProvider = "nullHandlingEnabled")
  public void countStarWithoutGroupBy(boolean nullHandlingEnabled) {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(nullHandlingEnabled)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1",
            "2",
            "null"
        )
        .andOnSecondInstance(
            "myField",
            "null",
            "null"
        )
        .whenQuery("select COUNT(*) from testTable")
        // COUNT(*) result should be the same regardless of whether null handling is enabled or not
        .thenResultIs("LONG", "5");
  }

  @Test(dataProvider = "nullHandlingEnabled")
  public void countLiteralWithoutGroupBy(boolean nullHandlingEnabled) {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(nullHandlingEnabled)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1",
            "2",
            "null"
        )
        .andOnSecondInstance(
            "myField",
            "null",
            "null"
        )
        .whenQuery("select COUNT('literal') from testTable")
        // COUNT(*) result should be the same regardless of whether null handling is enabled or not
        .thenResultIs("LONG", "5");
  }

  @Test
  public void countGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .addMetricField("value", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", 1},
            new Object[]{"tag2;tag3", null}
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", 1},
            new Object[]{"tag2;tag3", null}
        )
        .whenQuery("select tags, COUNT(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | LONG",
            "tag1    | 2",
            "tag2    | 4",
            "tag3    | 2"
        )
        .whenQueryWithNullHandlingEnabled("select tags, COUNT(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | LONG",
            "tag1    | 2",
            "tag2    | 2",
            "tag3    | 0"
        );
  }

  @DataProvider(name = "nullHandlingEnabled")
  public Object[][] nullHandlingEnabled() {
    return new Object[][]{
        {false}, {true}
    };
  }
}
