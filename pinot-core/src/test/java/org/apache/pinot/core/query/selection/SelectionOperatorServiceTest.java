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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.common.response.broker.SelectionResults;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.spi.utils.BytesUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/**
 * The <code>SelectionOperatorServiceTest</code> class provides unit tests for {@link SelectionOperatorUtils} and
 * {@link SelectionOperatorService}.
 */
public class SelectionOperatorServiceTest {
  private final String[] _columnNames =
      {"int", "long", "float", "double", "string", "int_array", "long_array", "float_array", "double_array", "string_array", "bytes"};
  private final DataSchema.ColumnDataType[] _columnDataTypes =
      {DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.FLOAT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.BYTES};
  private final DataSchema _dataSchema = new DataSchema(_columnNames, _columnDataTypes);
  private final DataSchema.ColumnDataType[] _compatibleColumnDataTypes =
      {DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.FLOAT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.FLOAT_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.BYTES};
  private final DataSchema _compatibleDataSchema = new DataSchema(_columnNames, _compatibleColumnDataTypes);
  private final DataSchema.ColumnDataType[] _upgradedColumnDataTypes =
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.LONG_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.DOUBLE_ARRAY, DataSchema.ColumnDataType.STRING_ARRAY, DataSchema.ColumnDataType.BYTES};
  private final DataSchema _upgradedDataSchema = new DataSchema(_columnNames, _upgradedColumnDataTypes);
  private final Object[] _row1 =
      {0, 1L, 2.0F, 3.0, "4", new int[]{5}, new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new String[]{"9"}, BytesUtils
          .toByteArray("1020")};
  private final Object[] _row2 =
      {10, 11L, 12.0F, 13.0, "14", new int[]{15}, new long[]{16L}, new float[]{17.0F}, new double[]{18.0}, new String[]{"19"}, BytesUtils
          .toByteArray("3040")};
  private final Object[] _compatibleRow1 =
      {1L, 2.0F, 3.0, 4, "5", new long[]{6L}, new float[]{7.0F}, new double[]{8.0}, new int[]{9}, new String[]{"10"}, BytesUtils
          .toByteArray("5060")};
  private final Object[] _compatibleRow2 =
      {11L, 12.0F, 13.0, 14, "15", new long[]{16L}, new float[]{17.0F}, new double[]{18.0}, new int[]{19}, new String[]{"20"}, BytesUtils
          .toByteArray("7000")};
  private QueryContext _queryContext;

  @BeforeClass
  public void setUp() {
    _queryContext = QueryContextConverterUtils.getQueryContextFromPQL(
        "SELECT " + String.join(", ", _columnNames) + " FROM testTable ORDER BY int DESC LIMIT 1, 2");
  }

  @Test
  public void testExtractExpressions() {
    IndexSegment indexSegment = mock(IndexSegment.class);
    when(indexSegment.getColumnNames()).thenReturn(new HashSet<>(Arrays.asList("foo", "bar", "foobar")));

    // For non 'SELECT *' select only queries, should return deduplicated selection expressions
    QueryContext queryContext = QueryContextConverterUtils
        .getQueryContextFromPQL("SELECT add(foo, 1), foo, sub(bar, 2 ), bar, foo, foobar, bar FROM testTable");
    List<ExpressionContext> expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 5);
    assertEquals(expressions.get(0).toString(), "add(foo,'1')");
    assertEquals(expressions.get(1).toString(), "foo");
    assertEquals(expressions.get(2).toString(), "sub(bar,'2')");
    assertEquals(expressions.get(3).toString(), "bar");
    assertEquals(expressions.get(4).toString(), "foobar");

    // For 'SELECT *' select only queries, should return all physical columns in alphabetical order
    queryContext = QueryContextConverterUtils.getQueryContextFromPQL("SELECT * FROM testTable");
    expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 3);
    assertEquals(expressions.get(0).toString(), "bar");
    assertEquals(expressions.get(1).toString(), "foo");
    assertEquals(expressions.get(2).toString(), "foobar");

    // For non 'SELECT *' select order-by queries, should return deduplicated order-by expressions followed by selection
    // expressions
    queryContext = QueryContextConverterUtils.getQueryContextFromPQL(
        "SELECT add(foo, 1), foo, sub(bar, 2 ), bar, foo, foobar, bar FROM testTable ORDER BY foo, sub(bar, 2)");
    expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 5);
    assertEquals(expressions.get(0).toString(), "foo");
    assertEquals(expressions.get(1).toString(), "sub(bar,'2')");
    assertEquals(expressions.get(2).toString(), "add(foo,'1')");
    assertEquals(expressions.get(3).toString(), "bar");
    assertEquals(expressions.get(4).toString(), "foobar");

    // For 'SELECT *' select order-by queries, should return deduplicated order-by expressions followed by all physical
    // columns in alphabetical order
    queryContext =
        QueryContextConverterUtils.getQueryContextFromPQL("SELECT * FROM testTable ORDER BY foo, sub(bar, 2)");
    expressions = SelectionOperatorUtils.extractExpressions(queryContext, indexSegment);
    assertEquals(expressions.size(), 4);
    assertEquals(expressions.get(0).toString(), "foo");
    assertEquals(expressions.get(1).toString(), "sub(bar,'2')");
    assertEquals(expressions.get(2).toString(), "bar");
    assertEquals(expressions.get(3).toString(), "foobar");
  }

  @Test
  public void testGetSelectionColumns() {
    // For non 'SELECT *', should return selection columns as is
    DataSchema dataSchema = mock(DataSchema.class);
    List<String> selectionColumns = SelectionOperatorUtils.getSelectionColumns(
        Arrays.asList(RequestContextUtils.getExpression("add(foo,'1')"),
            RequestContextUtils.getExpression("sub(bar,'2')"), RequestContextUtils.getExpression("foobar")),
        dataSchema);
    assertEquals(selectionColumns, Arrays.asList("add(foo,'1')", "sub(bar,'2')", "foobar"));

    // 'SELECT *' should return columns (no transform expressions) in alphabetical order
    when(dataSchema.getColumnNames()).thenReturn(new String[]{"add(foo,'1')", "sub(bar,'2')", "foo", "bar", "foobar"});
    selectionColumns = SelectionOperatorUtils
        .getSelectionColumns(Collections.singletonList(SelectionOperatorUtils.IDENTIFIER_STAR), dataSchema);
    assertEquals(selectionColumns, Arrays.asList("bar", "foo", "foobar"));

    // Test data schema from DataTableBuilder.buildEmptyDataTable()
    when(dataSchema.getColumnNames()).thenReturn(new String[]{"*"});
    selectionColumns = SelectionOperatorUtils
        .getSelectionColumns(Collections.singletonList(SelectionOperatorUtils.IDENTIFIER_STAR), dataSchema);
    assertEquals(selectionColumns, Collections.singletonList("*"));
  }

  @Test
  public void testCompatibleRowsMergeWithoutOrdering() {
    ArrayList<Object[]> mergedRows = new ArrayList<>(2);
    mergedRows.add(_row1);
    mergedRows.add(_row2);
    Collection<Object[]> rowsToMerge = new ArrayList<>(2);
    rowsToMerge.add(_compatibleRow1);
    rowsToMerge.add(_compatibleRow2);
    SelectionOperatorUtils.mergeWithoutOrdering(mergedRows, rowsToMerge, 3);
    assertEquals(mergedRows.size(), 3);
    assertSame(mergedRows.get(0), _row1);
    assertSame(mergedRows.get(1), _row2);
    assertSame(mergedRows.get(2), _compatibleRow1);
  }

  @Test
  public void testCompatibleRowsMergeWithOrdering() {
    SelectionOperatorService selectionOperatorService = new SelectionOperatorService(_queryContext, _dataSchema);
    PriorityQueue<Object[]> mergedRows = selectionOperatorService.getRows();
    int maxNumRows = _queryContext.getOffset() + _queryContext.getLimit();
    Collection<Object[]> rowsToMerge1 = new ArrayList<>(2);
    rowsToMerge1.add(_row1);
    rowsToMerge1.add(_row2);
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge1, maxNumRows);
    Collection<Object[]> rowsToMerge2 = new ArrayList<>(2);
    rowsToMerge2.add(_compatibleRow1);
    rowsToMerge2.add(_compatibleRow2);
    SelectionOperatorUtils.mergeWithOrdering(mergedRows, rowsToMerge2, maxNumRows);
    assertEquals(mergedRows.size(), 3);
    assertSame(mergedRows.poll(), _compatibleRow1);
    assertSame(mergedRows.poll(), _row2);
    assertSame(mergedRows.poll(), _compatibleRow2);
  }

  @Test
  public void testCompatibleRowsDataTableTransformation() throws Exception {
    Collection<Object[]> rows = new ArrayList<>(2);
    rows.add(_row1);
    rows.add(_compatibleRow1);
    DataSchema dataSchema = _dataSchema.clone();
    assertTrue(dataSchema.isTypeCompatibleWith(_compatibleDataSchema));
    dataSchema.upgradeToCover(_compatibleDataSchema);
    assertEquals(dataSchema, _upgradedDataSchema);
    DataTable dataTable = SelectionOperatorUtils.getDataTableFromRows(rows, dataSchema);
    Object[] expectedRow1 =
        {0L, 1.0, 2.0, 3.0, "4", new long[]{5L}, new double[]{6.0}, new double[]{7.0}, new double[]{8.0}, new String[]{"9"}, BytesUtils
            .toByteArray("1020")};
    Object[] expectedCompatibleRow1 =
        {1L, 2.0, 3.0, 4.0, "5", new long[]{6L}, new double[]{7.0}, new double[]{8.0}, new double[]{9.0}, new String[]{"10"}, BytesUtils
            .toByteArray("5060")};
    assertTrue(Arrays.deepEquals(SelectionOperatorUtils.extractRowFromDataTable(dataTable, 0), expectedRow1));
    assertTrue(Arrays.deepEquals(SelectionOperatorUtils.extractRowFromDataTable(dataTable, 1), expectedCompatibleRow1));
  }

  @Test
  public void testCompatibleRowsRenderSelectionResultsWithoutOrdering() {
    List<Object[]> rows = new ArrayList<>(2);
    rows.add(_row1);
    rows.add(_compatibleRow1);
    SelectionResults selectionResults = SelectionOperatorUtils.renderSelectionResultsWithoutOrdering(rows,
        _upgradedDataSchema, Arrays.asList(_columnNames), true);
    List<Serializable[]> resultRows = selectionResults.getRows();
    Serializable[] expectedRow1 =
        {0L, 1.0, 2.0, 3.0, "4", new long[]{5L}, new double[]{6.0}, new double[]{7.0}, new double[]{8.0}, new String[]{"9"}, "1020"};
    Serializable[] expectedRow2 =
        {1L, 2.0, 3.0, 4.0, "5", new long[]{6L}, new double[]{7.0}, new double[]{8.0}, new double[]{9.0}, new String[]{"10"}, "5060"};
    assertTrue(Arrays.deepEquals(resultRows.get(0), expectedRow1));
    assertTrue(Arrays.deepEquals(resultRows.get(1), expectedRow2));

    selectionResults = SelectionOperatorUtils.renderSelectionResultsWithoutOrdering(rows, _upgradedDataSchema,
        Arrays.asList(_columnNames), false);
    resultRows = selectionResults.getRows();
    Serializable[] expectedFormattedRow1 =
        {"0", "1.0", "2.0", "3.0", "4", new String[]{"5"}, new String[]{"6.0"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9"}, "1020"};
    Serializable[] expectedFormattedRow2 =
        {"1", "2.0", "3.0", "4.0", "5", new String[]{"6"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9.0"}, new String[]{"10"}, "5060"};
    assertTrue(Arrays.deepEquals(resultRows.get(0), expectedFormattedRow1));
    assertTrue(Arrays.deepEquals(resultRows.get(1), expectedFormattedRow2));
  }

  @Test
  public void testCompatibleRowsRenderSelectionResultsWithOrdering() {
    SelectionOperatorService selectionOperatorService =
        new SelectionOperatorService(_queryContext, _upgradedDataSchema);
    PriorityQueue<Object[]> rows = selectionOperatorService.getRows();
    rows.offer(_row1);
    rows.offer(_compatibleRow1);
    rows.offer(_compatibleRow2);
    SelectionResults selectionResults = selectionOperatorService.renderSelectionResultsWithOrdering(true);
    List<Serializable[]> resultRows = selectionResults.getRows();
    Serializable[] expectedRow1 =
        {1L, 2.0, 3.0, 4.0, "5", new long[]{6L}, new double[]{7.0}, new double[]{8.0}, new double[]{9.0}, new String[]{"10"}, "5060"};
    Serializable[] expectedRow2 =
        {0L, 1.0, 2.0, 3.0, "4", new long[]{5L}, new double[]{6.0}, new double[]{7.0}, new double[]{8.0}, new String[]{"9"}, "1020"};
    assertTrue(Arrays.deepEquals(resultRows.get(0), expectedRow1));
    assertTrue(Arrays.deepEquals(resultRows.get(1), expectedRow2));

    selectionOperatorService = new SelectionOperatorService(_queryContext, _upgradedDataSchema);
    rows = selectionOperatorService.getRows();
    rows.offer(_row1);
    rows.offer(_compatibleRow1);
    rows.offer(_compatibleRow2);
    selectionResults = selectionOperatorService.renderSelectionResultsWithOrdering(false);
    resultRows = selectionResults.getRows();
    Serializable[] expectedFormattedRow1 =
        {"1", "2.0", "3.0", "4.0", "5", new String[]{"6"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9.0"}, new String[]{"10"}, "5060"};
    Serializable[] expectedFormattedRow2 =
        {"0", "1.0", "2.0", "3.0", "4", new String[]{"5"}, new String[]{"6.0"}, new String[]{"7.0"}, new String[]{"8.0"}, new String[]{"9"}, "1020"};
    assertTrue(Arrays.deepEquals(resultRows.get(0), expectedFormattedRow1));
    assertTrue(Arrays.deepEquals(resultRows.get(1), expectedFormattedRow2));
  }
}
