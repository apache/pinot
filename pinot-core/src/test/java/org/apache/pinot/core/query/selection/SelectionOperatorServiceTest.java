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
package org.apache.pinot.core.query.selection;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.core.query.utils.OrderByComparatorFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * The <code>SelectionOperatorServiceTest</code> class provides unit tests for {@link SelectionOperatorUtils} and
 * {@link SelectionOperatorService}.
 */
public class SelectionOperatorServiceTest {
  private final String[] _columnNames = {
      "int", "long", "float", "double", "big_decimal", "string", "bytes", "int_array", "long_array", "float_array",
      "double_array", "string_array"
  };
  private final ColumnDataType[] _columnDataTypes = {
      ColumnDataType.INT, ColumnDataType.LONG, ColumnDataType.FLOAT, ColumnDataType.DOUBLE,
      ColumnDataType.BIG_DECIMAL, ColumnDataType.STRING, ColumnDataType.BYTES, ColumnDataType.INT_ARRAY,
      ColumnDataType.LONG_ARRAY, ColumnDataType.FLOAT_ARRAY, ColumnDataType.DOUBLE_ARRAY, ColumnDataType.STRING_ARRAY
  };
  private final DataSchema _dataSchema = new DataSchema(_columnNames, _columnDataTypes);
  private final Object[] _row1 = {
      0, 1L, 2.0F, 3.0, new BigDecimal(4), "5", BytesUtils.toByteArray(
      "0606"), new int[]{7}, new long[]{8L}, new float[]{9.0F}, new double[]{10.0}, new String[]{"11"}
  };
  private final Object[] _row2 = {
      10, 11L, 12.0F, 13.0, new BigDecimal(14), "15", BytesUtils.toByteArray(
      "1616"), new int[]{17}, new long[]{18L}, new float[]{19.0F}, new double[]{20.0}, new String[]{"21"}
  };
  private final Object[] _row3 = {
      1, 2L, 3.0F, 4.0, new BigDecimal(5), "6", BytesUtils.toByteArray(
      "0707"), new int[]{8}, new long[]{9L}, new float[]{10.0F}, new double[]{11.0}, new String[]{"12"}
  };
  private final Object[] _row4 = {
      11, 12L, 13.0F, 14.0, new BigDecimal(15), "16", BytesUtils.toByteArray(
      "1717"), new int[]{18}, new long[]{19L}, new float[]{20.0F}, new double[]{21.0}, new String[]{"22"}
  };
  private QueryContext _queryContext;

  @BeforeClass
  public void setUp() {
    _queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT " + String.join(", ", _columnNames) + " FROM testTable ORDER BY int DESC LIMIT 1, 2");
  }

  @Test
  public void testExtractExpressions() {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(new HashSet<>(Arrays.asList("foo", "bar", "foobar")));

    // For non 'SELECT *' select only queries, should return deduplicated selection expressions
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT add(foo, 1), foo, sub(bar, 2 ), bar, foo, foobar, bar FROM testTable");
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 5);
    assertEquals(expressions.get(0).toString(), "add(foo,1)");
    assertEquals(expressions.get(1).toString(), "foo");
    assertEquals(expressions.get(2).toString(), "sub(bar,2)");
    assertEquals(expressions.get(3).toString(), "bar");
    assertEquals(expressions.get(4).toString(), "foobar");

    // For 'SELECT *' select only queries, should return all physical columns in alphabetical order
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable");
    expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 3);
    assertEquals(expressions.get(0).toString(), "bar");
    assertEquals(expressions.get(1).toString(), "foo");
    assertEquals(expressions.get(2).toString(), "foobar");

    // For non 'SELECT *' select order-by queries, should return deduplicated order-by expressions followed by selection
    // expressions
    queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT add(foo, 1), foo, sub(bar, 2 ), bar, foo, foobar, bar FROM testTable ORDER BY foo, sub(bar, 2)");
    expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 5);
    assertEquals(expressions.get(0).toString(), "foo");
    assertEquals(expressions.get(1).toString(), "sub(bar,2)");
    assertEquals(expressions.get(2).toString(), "add(foo,1)");
    assertEquals(expressions.get(3).toString(), "bar");
    assertEquals(expressions.get(4).toString(), "foobar");

    // For 'SELECT *' select order-by queries, should return deduplicated order-by expressions followed by all physical
    // columns in alphabetical order
    queryContext = QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY foo, sub(bar, 2)");
    expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 4);
    assertEquals(expressions.get(0).toString(), "foo");
    assertEquals(expressions.get(1).toString(), "sub(bar,2)");
    assertEquals(expressions.get(2).toString(), "bar");
    assertEquals(expressions.get(3).toString(), "foobar");
  }

  @Test
  public void testGetSelectionColumns() {
    // For non 'SELECT *', should return selection columns as is
    DataSchema dataSchema = mock(DataSchema.class);
    List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(
        QueryContextConverterUtils.getQueryContext("SELECT add(foo, 1), sub(bar, 2), foobar FROM testTable"),
        dataSchema);
    assertEquals(selectionColumns, Arrays.asList("add(foo,1)", "sub(bar,2)", "foobar"));

    // 'SELECT *' should return columns (no transform expressions) in alphabetical order
    when(dataSchema.getColumnNames()).thenReturn(new String[]{"add(foo,1)", "sub(bar,2)", "foo", "bar", "foobar"});
    selectionColumns = SelectionOperatorUtils.getSelectionColumns(
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable ORDER BY add(foo, 1), sub(bar, 2), foo"),
        dataSchema);
    assertEquals(selectionColumns, Arrays.asList("bar", "foo", "foobar"));

    // Test data schema from DataTableBuilder.buildEmptyDataTable()
    when(dataSchema.getColumnNames()).thenReturn(new String[]{"*"});
    selectionColumns = SelectionOperatorUtils.getSelectionColumns(
        QueryContextConverterUtils.getQueryContext("SELECT * FROM testTable"), dataSchema);
    assertEquals(selectionColumns, new ArrayList<>(Collections.singletonList("*")));
  }

  @Test
  public void testRowsMergeWithoutOrdering() {
    List<Object[]> mergedRows = new ArrayList<>(2);
    mergedRows.add(_row1);
    mergedRows.add(_row2);
    SelectionResultsBlock mergedBlock = new SelectionResultsBlock(_dataSchema, mergedRows, _queryContext);
    List<Object[]> rowsToMerge = new ArrayList<>(2);
    rowsToMerge.add(_row3);
    rowsToMerge.add(_row4);
    SelectionResultsBlock blockToMerge = new SelectionResultsBlock(_dataSchema, rowsToMerge, _queryContext);
    SelectionOperatorUtils.mergeWithoutOrdering(mergedBlock, blockToMerge, 3);
    assertEquals(mergedRows.size(), 3);
    assertSame(mergedRows.get(0), _row1);
    assertSame(mergedRows.get(1), _row2);
    assertSame(mergedRows.get(2), _row3);
  }

  @Test
  public void testRowsMergeWithOrdering() {
    assertNotNull(_queryContext.getOrderByExpressions());
    Comparator<Object[]> comparator =
        OrderByComparatorFactory.getComparator(_queryContext.getOrderByExpressions(), false);
    int maxNumRows = _queryContext.getOffset() + _queryContext.getLimit();
    SelectionResultsBlock mergedBlock =
        new SelectionResultsBlock(_dataSchema, Collections.emptyList(), comparator, _queryContext);
    List<Object[]> rowsToMerge1 = Arrays.asList(_row2, _row1);
    SelectionResultsBlock blockToMerge1 =
        new SelectionResultsBlock(_dataSchema, rowsToMerge1, comparator, _queryContext);
    SelectionOperatorUtils.mergeWithOrdering(mergedBlock, blockToMerge1, maxNumRows);
    List<Object[]> rowsToMerge2 = Arrays.asList(_row4, _row3);
    SelectionResultsBlock blockToMerge2 =
        new SelectionResultsBlock(_dataSchema, rowsToMerge2, comparator, _queryContext);
    SelectionOperatorUtils.mergeWithOrdering(mergedBlock, blockToMerge2, maxNumRows);
    List<Object[]> mergedRows = mergedBlock.getRows();
    assertEquals(mergedRows.size(), 3);
    assertSame(mergedRows.get(0), _row4);
    assertSame(mergedRows.get(1), _row2);
    assertSame(mergedRows.get(2), _row3);
  }

  @Test
  public void testExtractRowFromDataTable()
      throws Exception {
    Collection<Object[]> rows = new ArrayList<>(2);
    rows.add(_row1);
    rows.add(_row2);
    DataTable dataTable = SelectionOperatorUtils.getDataTableFromRows(rows, _dataSchema, false);
    assertTrue(Arrays.deepEquals(SelectionOperatorUtils.extractRowFromDataTable(dataTable, 0), _row1));
    assertTrue(Arrays.deepEquals(SelectionOperatorUtils.extractRowFromDataTable(dataTable, 1), _row2));
  }
}
