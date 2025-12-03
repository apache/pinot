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
import org.testng.annotations.Test;


public class DistinctAvgAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void distinctAvgWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1",
            "2"
        )
        .andOnSecondInstance(
            "myField",
            "2",
            "null"
        )
        .whenQuery("select DISTINCT_AVG(myField) from testTable")
        .thenResultIs("DOUBLE",
            String.valueOf((3 + FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT) / 3.0)
        ).whenQueryWithNullHandlingEnabled("select DISTINCT_AVG(myField) from testTable")
        .thenResultIs("DOUBLE",
            "1.5"
        );
  }

  @Test
  public void distinctAvgWithGroupBy() {
    FluentQueryTest.withBaseDir(_baseDir)
        .withNullHandling(false)
        .givenTable(SINGLE_FIELD_NULLABLE_DIMENSION_SCHEMAS.get(FieldSpec.DataType.INT), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            "myField",
            "1",
            "2",
            "null"
        )
        .andOnSecondInstance(
            "myField",
            "2",
            "null"
        )
        .whenQuery("select myField, DISTINCT_AVG(myField) from testTable group by myField order by myField")
        .thenResultIs("INTEGER | DOUBLE",
            "-2147483648 | " + FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT,
            "1           | 1",
            "2           | 2"
        )
        .whenQueryWithNullHandlingEnabled(
            "select myField, DISTINCT_AVG(myField) from testTable  group by myField order by myField")
        .thenResultIs("INTEGER | DOUBLE",
            "1    | 1",
            "2    | 2",
            "null | null"
        );
  }

  @Test
  public void distinctAvgGroupByMV() {
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
        .whenQuery("select tags, DISTINCT_AVG(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1    | 1",
            "tag2    | 0.5",
            "tag3    | 0"
        )
        .whenQueryWithNullHandlingEnabled(
            "select tags, DISTINCT_AVG(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1    | 1",
            "tag2    | 1",
            "tag3    | null"
        );
  }

  @Test
  public void distinctAvgMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3;4"},
            new Object[]{"3;4;5;6"}
        )
        .andOnSecondInstance(
            new Object[]{"6;7;8;9"},
            new Object[]{"9;10;11;12"}
        )
        .whenQuery("select DISTINCT_AVG(mv) from testTable")
        .thenResultIs("DOUBLE", "6.5");
  }

  @Test
  public void distinctAvgMVWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"null"},
            new Object[]{"1;2"}
        )
        .andOnSecondInstance(
            new Object[]{"null"},
            new Object[]{"1;2"}
        )
        .whenQuery("select DISTINCT_AVG(mv) from testTable")
        .thenResultIs("DOUBLE", String.valueOf((3 + FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT) / 3.0))
        .whenQueryWithNullHandlingEnabled("select DISTINCT_AVG(mv) from testTable")
        .thenResultIs("DOUBLE", "1.5");
  }

  @Test
  public void distinctAvgMVGroupBySVWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .addSingleValueDimension("sv", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"null", "k1"}
        )
        .andOnSecondInstance(
            new Object[]{"null", "k1"},
            new Object[]{"1;2", "k1"}
        )
        .whenQuery("select DISTINCT_AVG(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", String.valueOf((3 + FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT) / 3.0))
        .whenQueryWithNullHandlingEnabled("select DISTINCT_AVG(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "1.5");
  }

  @Test
  public void distinctAvgMVGroupByMVWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv1", FieldSpec.DataType.INT)
                .addMultiValueDimension("mv2", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"null", "k1;k2"},
            new Object[]{"1;2", "k1;k2"}
        )
        .andOnSecondInstance(
            new Object[]{"null", "k1;k2"},
            new Object[]{"1;2", "k1;k2"}
        )
        .whenQuery("select DISTINCT_AVG(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", String.valueOf((3 + FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT) / 3.0),
            String.valueOf((3 + FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT) / 3.0))
        .whenQueryWithNullHandlingEnabled("select DISTINCT_AVG(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "1.5", "1.5");
  }
}
