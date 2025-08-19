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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.queries.FluentQueryTest;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MaxStringAggregationFunctionTest extends AbstractAggregationFunctionTest {

  /**
   * Helper method to create a FluentQueryTest builder for a table with a single String field.
   * This is used to simulate the DataTypeScenario concept from numeric aggregation tests,
   * but fixed for the STRING data type.
   */
  protected FluentQueryTest.DeclaringTable getDeclaringTable(boolean enableColumnBasedNullHandling) {
    return FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(enableColumnBasedNullHandling)
                .addSingleValueDimension("myField", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG);
  }

  @Test
  public void testNumericColumnExceptioninAggregateMethod() {
    ExpressionContext expression = RequestContextUtils.getExpression("column");
    MaxStringAggregationFunction function = new MaxStringAggregationFunction(Collections.singletonList(expression),
        false);

    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    BlockValSet mockBlockValSet = mock(BlockValSet.class);
    when(mockBlockValSet.getValueType()).thenReturn(FieldSpec.DataType.INT);
    blockValSetMap.put(expression, mockBlockValSet);

    try {
      function.aggregate(10, resultHolder, blockValSetMap);
      fail("Should throw BadQueryRequestException");
    } catch (BadQueryRequestException e) {
      assertTrue(e.getMessage().contains("Cannot compute MAXSTRING for numeric column"));
    }
  }

  @Test
  public void testNumericColumnExceptioninAggregateGroupBySVMethod() {
    ExpressionContext expression = RequestContextUtils.getExpression("column");
    MaxStringAggregationFunction function = new MaxStringAggregationFunction(Collections.singletonList(expression),
        false);

    GroupByResultHolder groupByResultHolder = function.createGroupByResultHolder(10, 20);
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    BlockValSet mockBlockValSet = mock(BlockValSet.class);
    when(mockBlockValSet.getValueType()).thenReturn(FieldSpec.DataType.INT);
    blockValSetMap.put(expression, mockBlockValSet);

    try {
      function.aggregateGroupBySV(10, new int[10], groupByResultHolder, blockValSetMap);
      fail("Should throw BadQueryRequestException");
    } catch (BadQueryRequestException e) {
      assertTrue(e.getMessage().contains("Cannot compute MAXSTRING for numeric column"));
    }
  }

  @Test
  public void testNumericColumnExceptioninAggregateGroupByMVMethod() {
    ExpressionContext expression = RequestContextUtils.getExpression("column");
    MaxStringAggregationFunction function = new MaxStringAggregationFunction(Collections.singletonList(expression),
        false);

    GroupByResultHolder groupByResultHolder = function.createGroupByResultHolder(10, 20);
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    BlockValSet mockBlockValSet = mock(BlockValSet.class);
    when(mockBlockValSet.getValueType()).thenReturn(FieldSpec.DataType.INT);
    blockValSetMap.put(expression, mockBlockValSet);

    try {
      function.aggregateGroupByMV(10, new int[10][], groupByResultHolder, blockValSetMap);
      fail("Should throw BadQueryRequestException");
    } catch (BadQueryRequestException e) {
      assertTrue(e.getMessage().contains("Cannot compute MAXSTRING for numeric column"));
    }
  }

  @Test
  public void testFunctionBasics() {
    ExpressionContext expression = RequestContextUtils.getExpression("column");
    MaxStringAggregationFunction function = new MaxStringAggregationFunction(Collections.singletonList(expression),
        false);

    // Test function type
    assertEquals(function.getType(), AggregationFunctionType.MAXSTRING);

    // Test string comparisons
    assertEquals(function.merge("apple", "banana"), "banana");
    assertEquals(function.merge("banana", "apple"), "banana");
    assertEquals(function.merge("", "apple"), "apple");
    assertEquals(function.merge("apple", ""), "apple");

    // Test null handling
    assertEquals(function.merge("apple", null), "apple");
    assertEquals(function.merge(null, "apple"), "apple");
    assertNull(function.merge(null, null));
    assertEquals(function.merge("apple", "null"), "null");

    // Test final result merging
    assertEquals(function.mergeFinalResult("apple", "banana"), "banana");
  }

  @Test
  void aggregationAllNullsWithNullHandlingDisabled() {
    // For MAXSTRING, when null handling is disabled, and all values are null,
    // the result should be 'null' as there's no valid string to compare.
    // This differs from numeric MAX/MIN which might return an initial default value.
    getDeclaringTable(false) // nullHandlingEnabled = false
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select maxstring(myField) from testTable")
        .thenResultIs("STRING", "\"null\""); // Asserting "null" as a string literal for the result
  }

  @Test
  void aggregationAllNullsWithNullHandlingEnabled() {
    // When null handling is enabled, and all values are null, the result should also be 'null'.
    getDeclaringTable(true) // nullHandlingEnabled = true
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select maxstring(myField) from testTable")
        .thenResultIs("STRING", "\"null\""); // Asserting "null" as a string literal for the result
  }

  @Test
  void aggregationGroupBySVAllNullsWithNullHandlingDisabled() {
    // For group by, if all values in a group are null and null handling is disabled,
    // the group's result for MAXSTRING should be 'null'.
    getDeclaringTable(false) // nullHandlingEnabled = false
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', maxstring(myField) from testTable group by 'literal'")
        // Expected "null" as a string literal for the aggregated column
        .thenResultIs("STRING | STRING", "literal | \"null\"");
  }

  @Test
  void aggregationGroupBySVAllNullsWithNullHandlingEnabled() {
    // For group by, if all values in a group are null and null handling is enabled,
    // the group's result for MAXSTRING should be 'null'.
    getDeclaringTable(true) // nullHandlingEnabled = true
        .onFirstInstance("myField",
            "null",
            "null"
        ).andOnSecondInstance("myField",
            "null"
        ).whenQuery("select 'literal', maxstring(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | \"null\"");
  }

  @Test
  void aggregationWithNullHandlingDisabled() {
    // With null handling disabled, null values are effectively skipped, and the maximum non-null
    // string should be found. The updated function handles this correctly.
    getDeclaringTable(false) // nullHandlingEnabled = false
        .onFirstInstance("myField",
            "cat",
            "null",
            "apple"
        ).andOnSecondInstance("myField",
            "null",
            "zebra",
            "null"
        ).whenQuery("select maxstring(myField) from testTable")
        .thenResultIs("STRING", "zebra"); // Max of {"cat", "apple", "zebra"} is "zebra"
  }

  @Test
  void aggregationWithNullHandlingEnabled() {
    // With null handling enabled, null values are explicitly ignored, and the maximum non-null
    // string should be found. The updated function handles this correctly.
    getDeclaringTable(true) // nullHandlingEnabled = true
        .onFirstInstance("myField",
            "cat",
            "null",
            "apple"
        ).andOnSecondInstance("myField",
            "null",
            "zebra",
            "null"
        ).whenQuery("select maxstring(myField) from testTable")
        .thenResultIs("STRING", "zebra"); // Max of {"cat", "apple", "zebra"} is "zebra"
  }

  @Test
  void aggregationGroupBySVWithNullHandlingDisabled() {
    // Group By on a single value (SV) column with mixed nulls and non-nulls.
    // Null handling disabled: nulls are ignored if there's at least one non-null value in the group.
    // The updated function should now correctly find the max among non-nulls.
    getDeclaringTable(false) // nullHandlingEnabled = false
        .onFirstInstance("myField",
            "alpha", // Grouped with 'literal'
            "null",  // Grouped with 'literal'
            "gamma"  // Grouped with 'literal'
        ).andOnSecondInstance("myField",
            "null",  // Grouped with 'literal'
            "beta",  // Grouped with 'literal'
            "null"   // Grouped with 'literal'
        ).whenQuery("select 'literal', maxstring(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING",
            "literal | \"null\""); // Max of {"alpha", null, "gamma", "beta"} is "null" when null handling is disabled
  }

  @Test
  void aggregationGroupBySVWithNullHandlingEnabled() {
    // Group By on a single value (SV) column with mixed nulls and non-nulls.
    // Null handling enabled: nulls are ignored.
    // The updated function should now correctly find the max among non-nulls.
    getDeclaringTable(true) // nullHandlingEnabled = true
        .onFirstInstance("myField",
            "alpha", // Grouped with 'literal'
            "null",  // Grouped with 'literal'
            "gamma"  // Grouped with 'literal'
        ).andOnSecondInstance("myField",
            "null",  // Grouped with 'literal'
            "beta",  // Grouped with 'literal'
            "null"   // Grouped with 'literal'
        ).whenQueryWithNullHandlingEnabled("select 'literal', maxstring(myField) from testTable group by 'literal'")
        .thenResultIs("STRING | STRING", "literal | gamma"); // Max of {"alpha", "gamma", "beta"} is "gamma"
  }

  @Test
  void aggregationGroupByMV() {
    FluentQueryTest.withBaseDir(_baseDir)
        .givenTable(
            new Schema.SchemaBuilder()
                .setSchemaName("testTable")
                .setEnableColumnBasedNullHandling(true) // Set at schema level for general behavior
                .addMultiValueDimension("tags", FieldSpec.DataType.STRING) // Dimension for tags
                .addDimensionField("value", FieldSpec.DataType.STRING)
                .build(), SINGLE_FIELD_TABLE_CONFIG)
        .onFirstInstance(
            new Object[]{"tag1;tag2", "banana"}, // Row 1: tag1 -> "banana", tag2 -> "banana"
            new Object[]{"tag2;tag3", null}      // Row 2: tag2 -> null, tag3 -> null
        )
        .andOnSecondInstance(
            new Object[]{"tag1;tag2", "apple"},   // Row 3: tag1 -> "apple", tag2 -> "apple"
            new Object[]{"tag2;tag3", "cherry"}  // Row 4: tag2 -> "cherry", tag3 -> "cherry"
        )
        // Query without explicit null handling enabled via query option (uses table schema setting or default)
        .whenQuery("select tags, MAXSTRING(value) from testTable group by tags order by tags")
        .thenResultIs(
            "STRING | STRING",
            "tag1    | banana", // Values for tag1: "banana", "apple". Max is "banana".
            "tag2    | \"null\"",
            // Values for tag2: "banana", "apple", null, "cherry". Max is "null" (nulls ignored). This is because
            // when null handling is disabled, the null value is read as "null" and we need to honor that
            "tag3    | \"null\""  // Values for tag3: null, "cherry". Max is "null".
        )
        // Query with explicit null handling enabled via query option
        .whenQueryWithNullHandlingEnabled("select tags, MAXSTRING(value) from testTable "
            + "group by tags order by tags")
        .thenResultIs(
            "STRING | STRING",
            "tag1    | banana", // Values for tag1: "banana", "apple". Max is "banana".
            "tag2    | cherry", // Values for tag2: "banana", "apple", "cherry". Max is "cherry".
            "tag3    | cherry"  // Values for tag3: "cherry". Max is "cherry".
        );
  }
}
