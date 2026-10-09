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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.core.data.table.Record;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class OrderByComparatorFactoryTest {
  private static final boolean ENABLE_NULL_HANDLING = true;
  private static final boolean ASC = true;
  private static final boolean DESC = false;
  private static final boolean NULLS_LAST = true;
  private static final boolean NULLS_FIRST = false;
  private static final ExpressionContext COLUMN1 = ExpressionContext.forIdentifier("Column1");
  private static final ExpressionContext COLUMN2 = ExpressionContext.forIdentifier("Column2");
  private static final int COLUMN1_INDEX = 0;
  private static final int COLUMN2_INDEX = 1;

  private List<Object[]> _rows;

  public void setUpSingleColumnRows() {
    _rows = Arrays.asList(new Object[]{1}, new Object[]{2}, new Object[]{null});
  }

  private List<Object> extractColumn(List<Object[]> rows, int columnIndex) {
    return rows.stream().map(row -> row[columnIndex]).collect(Collectors.toList());
  }

  @Test
  public void testAscNullsLast() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, ASC, NULLS_LAST));
    setUpSingleColumnRows();

    _rows.sort(OrderByComparatorFactory.getComparator(orderBys, ENABLE_NULL_HANDLING));

    assertEquals(extractColumn(_rows, COLUMN1_INDEX), Arrays.asList(1, 2, null));
  }

  @Test
  public void testAscNullsFirst() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, ASC, NULLS_FIRST));
    setUpSingleColumnRows();

    _rows.sort(OrderByComparatorFactory.getComparator(orderBys, ENABLE_NULL_HANDLING));

    assertEquals(extractColumn(_rows, COLUMN1_INDEX), Arrays.asList(null, 1, 2));
  }

  @Test
  public void testDescNullsLast() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, DESC, NULLS_LAST));
    setUpSingleColumnRows();

    _rows.sort(OrderByComparatorFactory.getComparator(orderBys, ENABLE_NULL_HANDLING));

    assertEquals(extractColumn(_rows, COLUMN1_INDEX), Arrays.asList(2, 1, null));
  }

  @Test
  public void testDescNullsFirst() {
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(COLUMN1, DESC, NULLS_FIRST));
    setUpSingleColumnRows();

    _rows.sort(OrderByComparatorFactory.getComparator(orderBys, ENABLE_NULL_HANDLING));

    assertEquals(extractColumn(_rows, COLUMN1_INDEX), Arrays.asList(null, 2, 1));
  }

  @Test
  public void testTwoNullsCompareNextColumn() {
    List<OrderByExpressionContext> orderBys = Arrays.asList(new OrderByExpressionContext(COLUMN1, ASC, NULLS_LAST),
        new OrderByExpressionContext(COLUMN2, ASC, NULLS_LAST));
    _rows = Arrays.asList(new Object[]{null, 2}, new Object[]{null, 3}, new Object[]{1, 1});

    _rows.sort(OrderByComparatorFactory.getComparator(orderBys, ENABLE_NULL_HANDLING));

    assertEquals(extractColumn(_rows, COLUMN2_INDEX), Arrays.asList(1, 2, 3));
  }

  private static ExpressionContext function(String name, String arg) {
    return ExpressionContext.forFunction(
        new FunctionContext(FunctionContext.Type.TRANSFORM, name, List.of(ExpressionContext.forIdentifier(arg))));
  }

  /// getRecordKeyComparator maps each ORDER BY expression to its position in the GROUP BY list. When several
  /// group-by keys are transform expressions rather than plain identifiers, every expression must still resolve to
  /// its own column, otherwise SortedRecordsMerger sees unequal groups as equal and merges them.
  @Test
  public void testRecordKeyComparatorWithMultipleTransformGroupByKeys() {
    ExpressionContext key0 = function("datetrunc", "tsColumn");
    ExpressionContext key1 = function("jsonextractindex", "jsonColumn");
    List<ExpressionContext> groupByExpressions = List.of(key0, key1);
    List<OrderByExpressionContext> orderBys =
        List.of(new OrderByExpressionContext(key0, ASC, NULLS_LAST), new OrderByExpressionContext(key1, ASC,
            NULLS_LAST));

    Comparator<Record> comparator =
        OrderByComparatorFactory.getRecordKeyComparator(orderBys, groupByExpressions, false);

    // Same second key, different first key: these are distinct groups and must not compare equal.
    Record a = new Record(new Object[]{1L, "x", 10.0});
    Record b = new Record(new Object[]{2L, "x", 20.0});
    assertTrue(comparator.compare(a, b) < 0, "rows differing only in the first group key compared equal");
    assertTrue(comparator.compare(b, a) > 0);

    // Same first key, different second key: also distinct groups.
    Record c = new Record(new Object[]{1L, "y", 30.0});
    assertTrue(comparator.compare(a, c) < 0, "rows differing only in the second group key compared equal");

    // Identical keys are the same group.
    assertEquals(comparator.compare(a, new Record(new Object[]{1L, "x", 99.0})), 0);
  }
}
