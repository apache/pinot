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
package org.apache.pinot.core.query.utils;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.query.aggregation.function.AnyValueAggregationFunction;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Verifies that typed query consumers preserve their raw-VARIANT and legacy opaque-type validation.
public class RawVariantQueryValidationTest {
  private static final boolean ENABLE_NULL_HANDLING = true;
  private static final boolean ASC = true;
  private static final boolean NULLS_LAST = true;
  private static final ExpressionContext COLUMN1 = ExpressionContext.forIdentifier("Column1");

  @Test
  void rejectsRawVariant() {
    ExpressionContext expression = ExpressionContext.forIdentifier("myField");
    AnyValueAggregationFunction function = new AnyValueAggregationFunction(List.of(expression), true);
    BlockValSet blockValSet = mock(BlockValSet.class);
    when(blockValSet.getValueType()).thenReturn(DataType.VARIANT);

    IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
        () -> function.aggregate(1, function.createAggregationResultHolder(), Map.of(expression, blockValSet)));

    assertEquals(exception.getMessage(),
        "ANY_VALUE does not support raw VARIANT values; extract a typed path with variantGet first");
  }

  @Test
  public void testRejectsRawVariant() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, ASC, NULLS_LAST));
    ColumnContext columnContext = mock(ColumnContext.class);
    when(columnContext.isSingleValue()).thenReturn(true);
    when(columnContext.getDataType()).thenReturn(DataType.VARIANT);

    BadQueryRequestException exception = expectThrows(BadQueryRequestException.class,
        () -> OrderByComparatorFactory.getComparator(orderBys, new ColumnContext[]{columnContext},
            ENABLE_NULL_HANDLING));
    assertTrue(exception.getMessage().contains("ORDER BY does not support raw VARIANT"));
  }

  @Test
  public void testAllowsUnknownForOrderByNull() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, ASC, NULLS_LAST));
    ColumnContext columnContext = mock(ColumnContext.class);
    when(columnContext.isSingleValue()).thenReturn(true);
    when(columnContext.getDataType()).thenReturn(DataType.UNKNOWN);

    assertEquals(OrderByComparatorFactory.getComparator(orderBys, new ColumnContext[]{columnContext},
        ENABLE_NULL_HANDLING).compare(new Object[]{null}, new Object[]{null}), 0);
  }

  @Test
  public void testPreservesPreExistingOpaqueTypeValidation() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, ASC, NULLS_LAST));
    for (DataType dataType : List.of(DataType.MAP, DataType.OPEN_STRUCT)) {
      ColumnContext columnContext = mock(ColumnContext.class);
      when(columnContext.isSingleValue()).thenReturn(true);
      when(columnContext.getDataType()).thenReturn(dataType);

      assertNotNull(OrderByComparatorFactory.getComparator(orderBys, new ColumnContext[]{columnContext},
          ENABLE_NULL_HANDLING), "ORDER BY validation should preserve prior behavior for " + dataType);
    }
  }
}
