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
package org.apache.pinot.core.query.aggregation.groupby;

import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.operator.BaseProjectOperator;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Unit tests for [DefaultGroupByExecutor#estimateBaseGroupCount], the base-group-count estimate that gates
/// grouping-set base aggregation vs. per-row expansion.
public class EstimateBaseGroupCountTest {

  /// Builds a project operator whose columns report the given dictionary cardinalities. A `null` cardinality
  /// models a non-dictionary column (not dictionary-encoded, no dictionary).
  private static BaseProjectOperator<?> mockProjectOperator(ExpressionContext[] expressions,
      Integer... cardinalities) {
    BaseProjectOperator<?> projectOperator = Mockito.mock(BaseProjectOperator.class);
    for (int i = 0; i < expressions.length; i++) {
      ColumnContext columnContext = Mockito.mock(ColumnContext.class);
      Integer cardinality = cardinalities[i];
      if (cardinality == null) {
        Mockito.when(columnContext.isDictionaryEncoded()).thenReturn(false);
        Mockito.when(columnContext.getDictionary()).thenReturn(null);
      } else {
        Dictionary dictionary = Mockito.mock(Dictionary.class);
        Mockito.when(dictionary.length()).thenReturn(cardinality);
        Mockito.when(columnContext.isDictionaryEncoded()).thenReturn(true);
        Mockito.when(columnContext.getDictionary()).thenReturn(dictionary);
      }
      Mockito.when(projectOperator.getResultColumnContext(expressions[i])).thenReturn(columnContext);
    }
    return projectOperator;
  }

  @Test
  public void testProductOfDictionaryCardinalities() {
    ExpressionContext a = ExpressionContext.forIdentifier("a");
    ExpressionContext b = ExpressionContext.forIdentifier("b");
    ExpressionContext[] expressions = {a, b};
    BaseProjectOperator<?> projectOperator = mockProjectOperator(expressions, 10, 20);
    assertEquals(DefaultGroupByExecutor.estimateBaseGroupCount(expressions, projectOperator), 200L);
  }

  @Test
  public void testNonDictionaryColumnReturnsMaxValue() {
    ExpressionContext a = ExpressionContext.forIdentifier("a");
    ExpressionContext b = ExpressionContext.forIdentifier("b");
    ExpressionContext[] expressions = {a, b};
    // Second column has no dictionary => cardinality unknown => estimate saturates so base aggregation is off.
    BaseProjectOperator<?> projectOperator = mockProjectOperator(expressions, 10, null);
    assertEquals(DefaultGroupByExecutor.estimateBaseGroupCount(expressions, projectOperator), Long.MAX_VALUE);
  }

  @Test
  public void testOverflowSaturatesToMaxValue() {
    ExpressionContext a = ExpressionContext.forIdentifier("a");
    ExpressionContext b = ExpressionContext.forIdentifier("b");
    ExpressionContext c = ExpressionContext.forIdentifier("c");
    ExpressionContext[] expressions = {a, b, c};
    // 2^31 * 2^31 * 2^31 overflows a long => saturates to MAX_VALUE rather than wrapping negative.
    BaseProjectOperator<?> projectOperator =
        mockProjectOperator(expressions, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
    assertEquals(DefaultGroupByExecutor.estimateBaseGroupCount(expressions, projectOperator), Long.MAX_VALUE);
  }

  @Test
  public void testZeroCardinalityColumnIgnored() {
    ExpressionContext a = ExpressionContext.forIdentifier("a");
    ExpressionContext b = ExpressionContext.forIdentifier("b");
    ExpressionContext[] expressions = {a, b};
    // An empty dictionary (cardinality 0) must not zero out the product nor divide-by-zero; it is skipped.
    BaseProjectOperator<?> projectOperator = mockProjectOperator(expressions, 0, 7);
    assertEquals(DefaultGroupByExecutor.estimateBaseGroupCount(expressions, projectOperator), 7L);
  }
}
