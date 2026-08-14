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

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class DistinctCountHLLPlusMVAggregationFunctionTest extends AbstractAggregationFunctionTest {
  private static final ExpressionContext BYTES_EXPRESSION = ExpressionContext.forIdentifier("bytesCol");

  @Test
  public void testAggregationMV() {
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
            new Object[]{"2;3;4"}
        )
        // Distinct values: 1, 2, 3, 4 = 4 distinct
        .whenQuery("select distinctcounthllplus(mv) from testTable")
        .thenResultIs("LONG", "4");
  }

  @Test
  public void testAggregationMVGroupBySV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("mv", FieldSpec.DataType.INT)
                .addSingleValueDimension("sv", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"1;2;3", "k1"},
            new Object[]{"4;5", "k2"}
        )
        .andOnSecondInstance(
            new Object[]{"2;3", "k1"},
            new Object[]{"5;6", "k2"}
        )
        .whenQuery("select sv, distinctcounthllplus(mv) from testTable group by sv order by sv")
        .thenResultIs("STRING | LONG",
            "k1 | 3",   // distinct: 1, 2, 3
            "k2 | 3");  // distinct: 4, 5, 6
  }

  @Test
  public void testAggregationMVGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true)
                .addMultiValueDimension("nums", FieldSpec.DataType.INT)
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            // Column order is alphabetical: nums, tags
            new Object[]{"1;2", "tag1;tag2"}
        )
        .andOnSecondInstance(
            new Object[]{"2;3", "tag1;tag2"}
        )
        .whenQuery("select tags, distinctcounthllplus(nums) from testTable group by tags order by tags")
        .thenResultIs("STRING | LONG",
            "tag1 | 3",   // distinct: 1, 2, 3
            "tag2 | 3");  // distinct: 1, 2, 3
  }

  @Test
  public void testMultiValueBytesUseMultiValueAccessor() {
    DistinctCountHLLPlusAggregationFunction function =
        new DistinctCountHLLPlusAggregationFunction(List.of(BYTES_EXPRESSION));
    byte[][][] bytesValues = {
        {{1}, {2}},
        {{2}, {3}},
        {{1}},
        {{4}}
    };
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.BYTES);
    when(blockValSet.isSingleValue()).thenReturn(false);
    when(blockValSet.isDictionaryEncoded()).thenReturn(false);
    when(blockValSet.getBytesValuesMV()).thenReturn(bytesValues);

    AggregationResultHolder aggregationResultHolder = function.createAggregationResultHolder();
    function.aggregate(bytesValues.length, aggregationResultHolder, Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, aggregationResultHolder), 4L);

    GroupByResultHolder groupBySVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupBySV(bytesValues.length, new int[]{0, 0, 1, 1}, groupBySVResultHolder,
        Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 0), 3L);
    Assert.assertEquals(extractFinalResult(function, groupBySVResultHolder, 1), 2L);

    GroupByResultHolder groupByMVResultHolder = function.createGroupByResultHolder(2, 2);
    function.aggregateGroupByMV(bytesValues.length, new int[][]{{0}, {1}, {0, 1}, {1}},
        groupByMVResultHolder, Map.of(BYTES_EXPRESSION, blockValSet));
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 0), 2L);
    Assert.assertEquals(extractFinalResult(function, groupByMVResultHolder, 1), 4L);

    verify(blockValSet, atLeastOnce()).getBytesValuesMV();
    verify(blockValSet, never()).getBytesValuesSV();
  }

  private static long extractFinalResult(DistinctCountHLLPlusAggregationFunction function,
      AggregationResultHolder resultHolder) {
    return function.extractFinalResult(function.extractAggregationResult(resultHolder));
  }

  private static long extractFinalResult(DistinctCountHLLPlusAggregationFunction function,
      GroupByResultHolder resultHolder, int groupKey) {
    return function.extractFinalResult(function.extractGroupByResult(resultHolder, groupKey));
  }
}
