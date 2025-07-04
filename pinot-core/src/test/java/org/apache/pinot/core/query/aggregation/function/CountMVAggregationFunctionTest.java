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


public class CountMVAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void basicCountMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3"},
            new Object[]{"4;5;6"}
        )
        .andOnSecondInstance(
            new Object[]{"7;8;9"},
            new Object[]{"10;11;12"}
        )
        .whenQuery("select count_mv(mv) from testTable")
        .thenResultIs("LONG", String.valueOf(12));
  }

  @Test
  public void countMVWithNulls() {
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
            new Object[]{"null"},
            new Object[]{"1;2"}
        )
        .whenQuery("select count_mv(mv) from testTable")
        .thenResultIs("LONG", "4")
        .whenQueryWithNullHandlingEnabled("select count_mv(mv) from testTable")
        .thenResultIs("LONG", "2");
  }

  @Test
  public void countMVGroupBySVWithNulls() {
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
        .whenQuery("select count_mv(mv) from testTable group by sv")
        .thenResultIs("LONG", "4")
        .whenQueryWithNullHandlingEnabled("select count_mv(mv) from testTable group by sv")
        .thenResultIs("LONG", "2");
  }

  @Test
  public void countMVGroupByMVWithNulls() {
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
            new Object[]{"null", "k1;k2"},
            new Object[]{"1;2", "k1;k2"}
        )
        .whenQuery("select count_mv(mv1) from testTable group by mv2")
        .thenResultIs("LONG", "4", "4")
        .whenQueryWithNullHandlingEnabled("select count_mv(mv1) from testTable group by mv2")
        .thenResultIs("LONG", "2", "2");
  }
}
