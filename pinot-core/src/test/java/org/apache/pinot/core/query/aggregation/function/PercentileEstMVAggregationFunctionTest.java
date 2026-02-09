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


public class PercentileEstMVAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void testAggregationMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.LONG)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3;4;5"}
        )
        .andOnSecondInstance(
            new Object[]{"6;7;8;9;10"}
        )
        // All values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 - p50 should be around 6
        .whenQuery("select percentileest(mv, 50) from testTable")
        .thenResultIs("LONG", "6");
  }

  @Test
  public void testAggregationMVGroupBySV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.LONG)
                .addSingleValueDimension("sv", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3;4;5", "k1"},
            new Object[]{"10;20;30", "k2"}
        )
        .andOnSecondInstance(
            new Object[]{"6;7;8;9;10", "k1"},
            new Object[]{"40;50", "k2"}
        )
        .whenQuery("select sv, percentileest(mv, 50) from testTable group by sv order by sv")
        .thenResultIs("STRING | LONG",
            "k1 | 6",    // values: 1-10, p50 ~= 6
            "k2 | 30");  // values: 10, 20, 30, 40, 50, p50 ~= 30
  }

  @Test
  public void testAggregationMVGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("nums", FieldSpec.DataType.LONG)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            // Column order is alphabetical: nums, tags
            new Object[]{"1;2;3", "tag1;tag2"}
        )
        .andOnSecondInstance(
            new Object[]{"4;5;6", "tag1;tag2"}
        )
        .whenQuery("select tags, percentileest(nums, 50) from testTable group by tags order by tags")
        .thenResultIs("STRING | LONG",
            "tag1 | 4",   // nums: 1, 2, 3, 4, 5, 6, p50 ~= 4
            "tag2 | 4");  // nums: 1, 2, 3, 4, 5, 6, p50 ~= 4
  }
}
