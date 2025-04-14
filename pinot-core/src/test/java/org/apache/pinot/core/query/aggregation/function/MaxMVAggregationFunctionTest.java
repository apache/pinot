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


public class MaxMVAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void aggregationAllNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"null"}
        )
        .andOnSecondInstance(
            new Object[]{"null"}
        )
        .whenQuery("select max_mv(mv) from testTable")
        .thenResultIs("DOUBLE",
            String.valueOf(
                (int) FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.INT, null))
        )
        .whenQueryWithNullHandlingEnabled("select max_mv(mv) from testTable")
        .thenResultIs("DOUBLE", "null");
  }

  @Test
  public void aggregationWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3"}
        )
        .andOnSecondInstance(
            new Object[]{"null"}
        )
        .whenQuery("select max_mv(mv) from testTable")
        .thenResultIs("DOUBLE", "3")
        .whenQueryWithNullHandlingEnabled("select max_mv(mv) from testTable")
        .thenResultIs("DOUBLE", "3");
  }

  @Test
  public void aggregationGroupBySVAllNulls() {
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
            new Object[]{"null", "k1"}
        )
        .whenQuery("select max_mv(mv) from testTable group by sv")
        .thenResultIs("DOUBLE",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.INT, null)))
        .whenQueryWithNullHandlingEnabled("select max_mv(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "null");
  }

  @Test
  public void aggregationGroupBySVWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
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
        .whenQuery("select max_mv(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "3", "3")
        .whenQueryWithNullHandlingEnabled("select max_mv(mv) from testTable group by sv")
        .thenResultIs("DOUBLE", "3", "3");
  }

  @Test
  public void aggregationGroupByMVAllNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv1", FieldSpec.DataType.INT)
                .addMultiValueDimension("mv2", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"null", "k1;k2"}
        )
        .andOnSecondInstance(
            new Object[]{"null", "k1;k2"}
        )
        .whenQuery("select max_mv(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE",
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.INT, null)),
            String.valueOf(FieldSpec.getDefaultNullValue(FieldSpec.FieldType.DIMENSION, FieldSpec.DataType.INT, null)))
        .whenQueryWithNullHandlingEnabled("select max_mv(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "null", "null");
  }

  @Test
  public void aggregationGroupByMVWithNulls() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv1", FieldSpec.DataType.INT)
                .addMultiValueDimension("mv2", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2", "k1;k2"}
        )
        .andOnSecondInstance(
            new Object[]{"null", "k1;k2"}
        )
        .whenQuery("select max_mv(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "2", "2")
        .whenQueryWithNullHandlingEnabled("select max_mv(mv1) from testTable group by mv2")
        .thenResultIs("DOUBLE", "2", "2");
  }
}
