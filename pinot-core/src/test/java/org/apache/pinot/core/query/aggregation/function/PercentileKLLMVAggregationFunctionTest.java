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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;


public class PercentileKLLMVAggregationFunctionTest extends AbstractAggregationFunctionTest {

  @Test
  public void testAggregationMV() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv", DataType.DOUBLE)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"1.0;2.0;3.0;4.0;5.0"})
        .andOnSecondInstance(new Object[]{"6.0;7.0;8.0;9.0;10.0"})
        // All values: 1-10, p50 should be around 5
        .whenQuery("select percentilekll(mv, 50) from testTable")
        .thenResultIs("DOUBLE", "5.0");
  }

  @Test
  public void testAggregationMVGroupBySV() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("mv", DataType.DOUBLE)
        .addSingleValueDimension("sv", DataType.STRING)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"1.0;2.0;3.0;4.0;5.0", "k1"}, new Object[]{"10.0;20.0;30.0", "k2"})
        .andOnSecondInstance(new Object[]{"6.0;7.0;8.0;9.0;10.0", "k1"}, new Object[]{"40.0;50.0", "k2"})
        .whenQuery("select sv, percentilekll(mv, 50) from testTable group by sv order by sv")
        .thenResultIs(
            "STRING | DOUBLE",
            "k1 | 5.0",   // values: 1-10, p50 ~= 5
            "k2 | 30.0"   // values: 10, 20, 30, 40, 50, p50 ~= 30
        );
  }

  @Test
  public void testAggregationMVGroupByMV() {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable")
        .setEnableColumnBasedNullHandling(true)
        .addMultiValueDimension("nums", DataType.DOUBLE)
        .addMultiValueDimension("tags", DataType.STRING)
        .build();
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(schema, SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(new Object[]{"1.0;2.0;3.0", "tag1;tag2"})  // Column order is alphabetical: nums, tags
        .andOnSecondInstance(new Object[]{"4.0;5.0;6.0", "tag1;tag2"})
        .whenQuery("select tags, percentilekll(nums, 50) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | DOUBLE",
            "tag1 | 3.0",  // nums: 1, 2, 3, 4, 5, 6, p50 ~= 3
            "tag2 | 3.0"   // nums: 1, 2, 3, 4, 5, 6, p50 ~= 3
        );
  }
}
