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
package org.apache.pinot.queries;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class NullHandlingEnabledQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "NullHandlingEnabledQueriesTest");
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final String COLUMN1 = "column1";
  private static final String COLUMN2 = "column2";
  private static final TableConfig TABLE_CONFIG =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).build();
  private static final TableConfig TABLE_CONFIG_WITH_RAW_COLUMN =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
          .setNoDictionaryColumns(List.of(COLUMN1))
          .build();
  private static final TableConfig TABLE_CONFIG_WITH_SORTED_COLUMN =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setSortedColumn(COLUMN1).build();
  private static final TableConfig TABLE_CONFIG_WITH_INVERTED_INDEX_COLUMN =
      new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
          .setInvertedIndexColumns(List.of(COLUMN1))
          .build();
  private static final int NUM_OF_SEGMENT_COPIES = 4;
  private static final Map<String, String> QUERY_OPTIONS = Map.of("enableNullHandling", "true");

  private final List<GenericRow> _rows = new ArrayList<>();
  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @Override
  protected String getFilter() {
    return "";
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  private void setUpSegments(TableConfig tableConfig, Schema schema)
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    segmentGeneratorConfig.setTableName(RAW_TABLE_NAME);
    segmentGeneratorConfig.setSegmentName(SEGMENT_NAME);
    segmentGeneratorConfig.setDefaultNullHandlingEnabled(true);
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getPath());

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, new GenericRowRecordReader(_rows));
    driver.build();

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), ReadMode.mmap);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  private void initializeRows() {
    _rows.clear();
  }

  private void insertRow(Object value) {
    GenericRow row = new GenericRow();
    row.putValue(COLUMN1, value);
    _rows.add(row);
  }

  private void insertRowWithTwoColumns(Object column1Value, Object column2Value) {
    GenericRow row = new GenericRow();
    row.putValue(COLUMN1, column1Value);
    row.putValue(COLUMN2, column2Value);
    _rows.add(row);
  }

  private static Schema.SchemaBuilder schemaBuilder() {
    return new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME);
  }

  /// Returns the result rows of the given query, executed with null handling enabled.
  private List<Object[]> getRows(String query) {
    return getRows(query, QUERY_OPTIONS);
  }

  private List<Object[]> getRows(String query, @Nullable Map<String, String> queryOptions) {
    //noinspection LanguageMismatch
    ResultTable resultTable = getBrokerResponse(query, queryOptions).getResultTable();
    assertNotNull(resultTable, "Failed to get result table for query: " + query);
    return resultTable.getRows();
  }

  @DataProvider(name = "BooleanAssertionFunctions")
  public static Object[][] getBooleanAssertionFunctionsParameters() {
    return new Object[][]{
        {"istrue", true, true},
        {"istrue", false, false},
        {"istrue", null, false},
        {"isnottrue", true, false},
        {"isnottrue", false, true},
        {"isnottrue", null, true},
        {"isfalse", true, false},
        {"isfalse", false, true},
        {"isfalse", null, false},
        {"isnotfalse", true, true},
        {"isnotfalse", false, false},
        {"isnotfalse", null, true}
    };
  }

  @Test(dataProvider = "BooleanAssertionFunctions")
  public void testBooleanAssertionFunctions(String function, Boolean data, Boolean queryResult)
      throws Exception {
    initializeRows();
    insertRow(data);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.BOOLEAN).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT %s(%s) FROM testTable LIMIT 1", function, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.get(0)[0], queryResult);
  }

  @Test
  public void testGroupByOrderByNullsLastUsingOrdinal()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(null);
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(2);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 DESC NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{2, (long) 2 * NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, (long) 3 * NUM_OF_SEGMENT_COPIES});
  }

  /// A row holding the column's default null value must not join the null group.
  ///
  /// Ingestion stores that default in the forward index for a null row, so both share a dictionary id and only the
  /// null vector tells them apart. Grouping therefore has to move nulls onto an id of their own.
  @Test
  public void testGroupByKeepsNullApartFromTheDefaultNullValue()
      throws Exception {
    initializeRows();
    insertRow(1);
    insertRow(null);
    insertRow(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0),
        new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  /// A multi-value row expands into one group per value it holds, so a row whose column is null contributes a single
  /// null group rather than reading as the column's default null value, which is what a null row is stored as.
  @Test
  public void testGroupByMultiValueColumnHoldingNulls()
      throws Exception {
    initializeRows();
    insertRow(new Object[]{1, 2});
    insertRow(new Object[]{2, 3});
    insertRow(null);
    insertRow(null);
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{2, (long) 2 * NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{3, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(3), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  /// A column with no dictionary resolves group keys through an on-the-fly dictionary rather than dictionary ids,
  /// and its null rows still hold the column's default null value, so the same null-vs-default separation applies.
  @Test
  public void testGroupByRawColumnKeepsNullApartFromTheDefaultNullValue()
      throws Exception {
    initializeRows();
    insertRow(1);
    insertRow(null);
    insertRow(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0),
        new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  /// A STRING group key is held in an object-keyed map, which stores the null group under a null key instead of
  /// tracking it beside the map the way the primitive-keyed maps do.
  @Test
  public void testGroupByRawStringColumnHoldingNulls()
      throws Exception {
    initializeRows();
    insertRow("a");
    insertRow("b");
    insertRow(null);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.STRING).build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{"a", (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{"b", (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  /// A dictionary-backed transform must keep a null apart from the column's default null value, exactly as reading
  /// the column directly does.
  ///
  /// A transform hands out its own null bitmap but exposes no data source to inspect, so nullability cannot be ruled
  /// out for it the way it can for a column read straight from a segment.
  @Test
  public void testGroupByDictionaryBackedTransformKeepsNullApartFromTheDefaultNullValue()
      throws Exception {
    initializeRows();
    insertRow(new Object[]{1});
    insertRow(null);
    insertRow(new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String transform = String.format("filterMv(%s, 'v <= 1')", COLUMN1);
    String query = String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", transform,
        transform);

    List<Object[]> rows = getRows(query);

    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0)[0], FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT);
    assertEquals(rows.get(1)[0], 1);
    assertNull(rows.get(2)[0], "The null row must not join the group of the column's default null value");
  }

  /// Grouping sets resolve each column's values to ids of their own, and the multi-value resolver has to recognize a
  /// null row there too, so it forms its own group instead of joining the column's default null value.
  @Test
  public void testGroupingSetsOverMultiValueColumnHoldingNulls()
      throws Exception {
    initializeRows();
    insertRow(new Object[]{1});
    insertRow(null);
    insertRow(new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT});
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format(
        "SELECT %s, COUNT(*) FROM testTable GROUP BY GROUPING SETS ((%s)) ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);

    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT,
        (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, (long) NUM_OF_SEGMENT_COPIES});
  }

  /// The group id of a null value is handed out beside the group key map, so a generator that counted only the map
  /// would under-report its groups and let a group id run past the result holder it sized.
  @Test
  public void testNullGroupIsCountedWhenSizingTheResultHolder()
      throws Exception {
    initializeRows();
    insertRow(new Object[]{1});
    insertRow(new Object[]{2});
    insertRow(null);
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);
    // Sized so that the null group is the first id past the holder's initial capacity
    Map<String, String> queryOptions =
        Map.of("enableNullHandling", "true", "maxInitialResultHolderCapacity", "2");

    List<Object[]> rows = getRows(query, queryOptions);

    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{2, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, (long) NUM_OF_SEGMENT_COPIES});
  }

  /// One column without a dictionary sends both columns down the on-the-fly path, where a null contributes a reserved
  /// id to the composed key, in any combination across the two columns.
  @Test
  public void testGroupByRawAndDictionaryColumnsHoldingNulls()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 10);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(null, 10);
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, %s, COUNT(*) FROM testTable GROUP BY %s, %s ORDER BY 1 NULLS LAST, 2 NULLS LAST",
            COLUMN1, COLUMN2, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, null, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(3), new Object[]{null, null, (long) NUM_OF_SEGMENT_COPIES});
  }

  /// A column with no dictionary is grouped through an on-the-fly one, and a multi-value column of that kind takes
  /// the path that reads whole rows at a time, which has to consult the null bitmap just as the others do.
  @Test
  public void testGroupByRawMultiValueColumnHoldingNulls()
      throws Exception {
    initializeRows();
    insertRow(new Object[]{1, 2});
    insertRow(new Object[]{2, 3});
    insertRow(null);
    insertRow(null);
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, COUNT(*) FROM testTable GROUP BY %s ORDER BY 1 NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{2, (long) 2 * NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{3, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(3), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  /// Once the group limit is reached, a row of an existing group must still land in that group. The at-limit path
  /// resolves each column's key value without being allowed to create one, and has to compose the looked-up values
  /// rather than whatever the key buffer held before.
  @Test
  public void testGroupLimitStillRoutesRowsToTheirExistingGroup()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 10);
    insertRowWithTwoColumns(2, 20);
    insertRowWithTwoColumns(2, 20);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, %s, COUNT(*) FROM testTable GROUP BY %s, %s ORDER BY 1", COLUMN1, COLUMN2, COLUMN1,
            COLUMN2);
    Map<String, String> queryOptions = Map.of("enableNullHandling", "true", "numGroupsLimit", "2");

    List<Object[]> rows = getRows(query, queryOptions);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{1, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{2, 20, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  /// One column without a dictionary sends every column down the on-the-fly path, so a null has to be recognized
  /// there for the dictionary-encoded column beside it as well.
  @Test
  public void testGroupByRawMultiValueColumnBesideADictionaryColumn()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(new Object[]{1, 2}, 10);
    insertRowWithTwoColumns(null, 10);
    insertRowWithTwoColumns(new Object[]{1}, null);
    Schema schema = schemaBuilder()
        .addMultiValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT %s, %s, COUNT(*) FROM testTable GROUP BY %s, %s ORDER BY 1 NULLS LAST, 2 NULLS LAST",
            COLUMN1, COLUMN2, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, null, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{2, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(3), new Object[]{null, 10, (long) NUM_OF_SEGMENT_COPIES});
  }

  /// Several columns compose their dictionary ids into a single key using each column's cardinality, so the id
  /// reserved for null has to be counted in every one of them for the composition to stay unambiguous.
  @Test
  public void testGroupByTwoColumnsHoldingNulls()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 10);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(null, 10);
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s, COUNT(*) FROM testTable GROUP BY %s, %s ORDER BY 1 NULLS LAST, 2 NULLS LAST",
            COLUMN1, COLUMN2, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(1), new Object[]{1, null, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(2), new Object[]{null, 10, (long) NUM_OF_SEGMENT_COPIES});
    assertEquals(rows.get(3), new Object[]{null, null, (long) NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testHavingFilterIsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(%s) FROM testTable GROUP BY %s HAVING %s IS NULL LIMIT 100", COLUMN1, COLUMN2,
            COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testHavingFilterIsNotNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(%s) FROM testTable GROUP BY %s HAVING %s IS NOT NULL LIMIT 100", COLUMN1,
            COLUMN2, COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, (long) NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testHavingFilterNotOfColumnIsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(true, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(%s) FROM testTable GROUP BY %s HAVING (NOT %s) IS NULL LIMIT 100", COLUMN1,
            COLUMN2, COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testHavingFilterNotColumnIsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(true, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, COUNT(%s) FROM testTable GROUP BY %s HAVING NOT (%s IS NULL) LIMIT 100", COLUMN1,
            COLUMN2, COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{true, (long) NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testHavingFilterIsNullAndIsNotNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format(
        "SELECT %s, COUNT(%s) FROM testTable GROUP BY %s HAVING (%s IS NULL) AND (COUNT(%s) is NOT NULL) LIMIT 100",
        COLUMN1, COLUMN2, COLUMN1, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testHavingFilterIsNullOrIsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format(
        "SELECT %s, COUNT(%s) FROM testTable GROUP BY %s HAVING (%s IS NULL) OR (COUNT(%s) is NULL) LIMIT 100", COLUMN1,
        COLUMN2, COLUMN1, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{null, (long) 2 * NUM_OF_SEGMENT_COPIES});
  }

  @Test
  public void testSelectDistinctOrderByNullsFirst()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    initializeRows();
    insertRow(1);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS FIRST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertNull(rows.get(0)[0]);
    assertNotNull(rows.get(1)[0]);
  }

  @Test
  public void testSelectDistinctOrderByNullsLast()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    initializeRows();
    insertRow(1);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS LAST", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertNotNull(rows.get(0)[0]);
    assertNull(rows.get(1)[0]);
  }

  @Test
  public void testSelectDistinctIntegerMinValueDiffersFromNull()
      throws Exception {
    FileUtils.deleteDirectory(INDEX_DIR);
    initializeRows();
    insertRow(Integer.MIN_VALUE);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 2);
  }

  @Test
  public void testSelectDistinctMultiColumn()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s,%s FROM testTable", COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
  }

  @Test
  public void testSelectDistinctOrderByMultiColumn()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT DISTINCT %s,%s FROM testTable ORDER BY %s,%s", COLUMN1, COLUMN2, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{1, 1});
    assertEquals(rows.get(1), new Object[]{null, 1});
    assertEquals(rows.get(2), new Object[]{null, 2});
    assertEquals(rows.get(3), new Object[]{null, null});
  }

  @Test
  public void testSelectDistinctOrderByMultiColumnCustomNullOrdering()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT DISTINCT %s,%s FROM testTable ORDER BY %s NULLS FIRST, %s DESC NULLS LAST", COLUMN1,
            COLUMN2, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{null, 2});
    assertEquals(rows.get(1), new Object[]{null, 1});
    assertEquals(rows.get(2), new Object[]{null, null});
    assertEquals(rows.get(3), new Object[]{1, 1});
  }

  @DataProvider(name = "NumberTypes")
  public static Object[][] getPrimitiveDataTypes() {
    return new Object[][]{
        {DataType.INT}, {DataType.LONG}, {DataType.DOUBLE}, {DataType.FLOAT}
    };
  }

  @Test(dataProvider = "NumberTypes")
  public void testSelectDistinctWithLimit(DataType dataType)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(3);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT 3", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
  }

  @Test(dataProvider = "NumberTypes")
  public void testSelectDistinctOrderByWithLimit(DataType dataType)
      throws Exception {
    double delta = 0.01;
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(3);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s LIMIT 3", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
    assertTrue(Math.abs(((Number) rows.get(0)[0]).doubleValue() - 1.0) < delta);
    assertTrue(Math.abs(((Number) rows.get(1)[0]).doubleValue() - 2.0) < delta);
    assertTrue(Math.abs(((Number) rows.get(2)[0]).doubleValue() - 3.0) < delta);
  }

  @DataProvider(name = "ObjectTypes")
  public static Object[][] getObjectDataTypes() {
    return new Object[][]{
        {DataType.STRING, "a"},
        {DataType.BIG_DECIMAL, 1},
        {DataType.BYTES, "a string".getBytes()}
    };
  }

  @Test(dataProvider = "ObjectTypes")
  public void testObjectSingleColumnDistinctOrderByNullsFirst(DataType dataType, Object value)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(value);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS FIRST LIMIT 1", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertNull(rows.get(0)[0]);
  }

  @Test(dataProvider = "ObjectTypes")
  public void testObjectSingleColumnDistinctOrderByNullsLast(DataType dataType, Object value)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(value);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCT %s FROM testTable ORDER BY %s NULLS LAST LIMIT 1", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertNotNull(rows.get(0)[0]);
  }

  @Test(dataProvider = "NumberTypes")
  public void testDistinctCountDictNumberTypes(DataType dataType)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCTCOUNT(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 1);
  }

  @Test(dataProvider = "NumberTypes")
  public void testDistinctCountNonDictNumberTypes(DataType dataType)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query = String.format("SELECT DISTINCTCOUNT(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 1);
  }

  @Test(dataProvider = "NumberTypes")
  public void testGroupByDistinctCountDictNumberTypes(DataType dataType)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, "key");
    insertRowWithTwoColumns(1, "key");
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addSingleValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCTCOUNT(%s), %s FROM testTable GROUP BY %s", COLUMN1, COLUMN2, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, "key"});
  }

  @Test(dataProvider = "NumberTypes")
  public void testGroupByDistinctCountNonDictNumberTypes(DataType dataType)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, "key");
    insertRowWithTwoColumns(1, "key");
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addSingleValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query = String.format("SELECT DISTINCTCOUNT(%s), %s FROM testTable GROUP BY %s", COLUMN1, COLUMN2, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, "key"});
  }

  @Test(dataProvider = "NumberTypes")
  public void testGroupByMvDistinctCountNonDictNumberTypes(DataType dataType)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, new String[]{"key1", "key2"});
    insertRowWithTwoColumns(1, new String[]{"key1", "key2"});
    insertRowWithTwoColumns(2, new String[]{"key2"});
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addMultiValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG_WITH_RAW_COLUMN, schema);
    String query =
        String.format("SELECT DISTINCTCOUNT(%s), %s FROM testTable GROUP BY %s ORDER BY %s", COLUMN1, COLUMN2, COLUMN2,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.get(0), new Object[]{1, "key1"});
    assertEquals(rows.get(1), new Object[]{2, "key2"});
  }

  @Test(dataProvider = "NumberTypes")
  public void testGroupByMvDistinctCountDictNumberTypes(DataType dataType)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, new String[]{"key1", "key2"});
    insertRowWithTwoColumns(1, new String[]{"key1", "key2"});
    insertRowWithTwoColumns(2, new String[]{"key2"});
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addMultiValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT DISTINCTCOUNT(%s), %s FROM testTable GROUP BY %s ORDER BY %s", COLUMN1, COLUMN2, COLUMN2,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.get(0), new Object[]{1, "key1"});
    assertEquals(rows.get(1), new Object[]{2, "key2"});
  }

  @DataProvider(name = "DistinctCountObjectTypes")
  public static Object[][] getDistinctAggregationObjectTypes() {
    return new Object[][]{
        {DataType.STRING, "a"},
        {DataType.BYTES, "a string".getBytes()}
    };
  }

  @Test(dataProvider = "DistinctCountObjectTypes")
  public void testObjectDistinctCountObjectTypes(DataType dataType, Object value)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(value);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCTCOUNT(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 1);
  }

  @Test(dataProvider = "DistinctCountObjectTypes")
  public void testGroupByDistinctCountObjectTypes(DataType dataType, Object value)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, "key");
    insertRowWithTwoColumns(value, "key");
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addSingleValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCTCOUNT(%s), %s FROM testTable GROUP BY %s", COLUMN1, COLUMN2, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{1, "key"});
  }

  @Test
  public void testDistinctSum()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(2);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCTSUM(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], (double) 3);
  }

  @Test
  public void testDistinctAvg()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    insertRow(2);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT DISTINCTAVG(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 1.5);
  }

  @Test
  public void testTransformBlockValSetGetNullBitmap()
      throws Exception {
    initializeRows();
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT (CASE WHEN %s IS NULL THEN 1 END) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0)[0], 1);
  }

  private boolean contains(List<Object[]> rows, Object[] target) {
    for (Object[] row : rows) {
      if (Arrays.equals(row, target)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testMultiColumnGroupBy()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(1, Integer.MIN_VALUE);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT count(*), %s, %s FROM testTable GROUP BY %s, %s", COLUMN1, COLUMN2, COLUMN1,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 5);
    assertTrue(contains(rows, new Object[]{(long) NUM_OF_SEGMENT_COPIES, null, null}));
    assertTrue(contains(rows, new Object[]{(long) 2 * NUM_OF_SEGMENT_COPIES, null, 1}));
    assertTrue(contains(rows, new Object[]{(long) NUM_OF_SEGMENT_COPIES, 1, 1}));
    assertTrue(contains(rows, new Object[]{(long) NUM_OF_SEGMENT_COPIES, 1, null}));
    assertTrue(contains(rows, new Object[]{(long) NUM_OF_SEGMENT_COPIES, 1, Integer.MIN_VALUE}));
  }

  @Test
  public void testMultiColumnGroupByWithLimit()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, 1);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(1, Integer.MIN_VALUE);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT count(*), %s, %s FROM testTable GROUP BY %s, %s LIMIT 3", COLUMN1, COLUMN2, COLUMN1,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
  }

  @Test
  public void testGroupByOrderBy()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(1);
    insertRow(2);
    insertRow(3);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT count(*), %s FROM testTable GROUP BY %s ORDER BY %s ASC NULLS LAST", COLUMN1, COLUMN1,
            COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 4);
    assertEquals(rows.get(0), new Object[]{(long) 2 * NUM_OF_SEGMENT_COPIES, 1});
    assertEquals(rows.get(1), new Object[]{(long) NUM_OF_SEGMENT_COPIES, 2});
    assertEquals(rows.get(2), new Object[]{(long) NUM_OF_SEGMENT_COPIES, 3});
    assertEquals(rows.get(3), new Object[]{(long) NUM_OF_SEGMENT_COPIES, null});
  }

  @Test
  public void testGroupByOrderByWithLimit()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(1);
    insertRow(2);
    insertRow(3);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT count(*), %s FROM testTable GROUP BY %s ORDER BY %s DESC NULLS FIRST LIMIT 3", COLUMN1,
            COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3);
    assertEquals(rows.get(0), new Object[]{(long) NUM_OF_SEGMENT_COPIES, null});
    assertEquals(rows.get(1), new Object[]{(long) NUM_OF_SEGMENT_COPIES, 3});
    assertEquals(rows.get(2), new Object[]{(long) NUM_OF_SEGMENT_COPIES, 2});
  }

  @Test
  public void testNestedCaseTransformFunction()
      throws Exception {
    initializeRows();
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT (CASE WHEN %s = -2147483648 THEN 1 ELSE 2 END) + 0 FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.get(0), new Object[]{(double) 2});
  }

  @Test
  public void testFilteringOnInvertedIndexColumn()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(false);
    insertRow(true);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.BOOLEAN).build();
    setUpSegments(TABLE_CONFIG_WITH_INVERTED_INDEX_COLUMN, schema);
    String query =
        String.format("SELECT * FROM testTable WHERE %s = false", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{false});
  }

  @Test
  public void testFilteringOnSortedColumn()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(false);
    insertRow(true);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.BOOLEAN).build();
    setUpSegments(TABLE_CONFIG_WITH_SORTED_COLUMN, schema);
    String query =
        String.format("SELECT * FROM testTable WHERE %s = false", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{false});
  }

  @Test
  public void testRangeFiltering()
      throws Exception {
    initializeRows();
    insertRow(-1);
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG_WITH_SORTED_COLUMN, schema);
    String query = String.format("SELECT * FROM testTable WHERE %s < 0", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{-1});
  }

  @Test
  public void testEqualFiltering()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(Integer.MIN_VALUE);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG_WITH_SORTED_COLUMN, schema);
    String query = String.format("SELECT * FROM testTable WHERE %s = %d", COLUMN1, Integer.MIN_VALUE);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{Integer.MIN_VALUE});
  }

  @Test
  public void testOrFiltering()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, -1);
    insertRowWithTwoColumns(-1, null);
    insertRowWithTwoColumns(-1, 1);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(null, -1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE OR(%s > 0, %s < 0) LIMIT 100", COLUMN1, COLUMN2, COLUMN1,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES * 3);
  }

  @Test
  public void testNotFiltering()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(-1);
    insertRow(1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT %s FROM testTable WHERE NOT(%s = 1) LIMIT 100", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{-1});
  }

  @Test
  public void testNotAndFiltering()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, -1);
    insertRowWithTwoColumns(-1, null);
    insertRowWithTwoColumns(-1, 1);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(null, -1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE NOT(AND(%s > 0, %s < 0)) LIMIT 100", COLUMN1, COLUMN2,
            COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 3 * NUM_OF_SEGMENT_COPIES);
  }

  @Test
  public void testNotOrFiltering()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, -1);
    insertRowWithTwoColumns(-1, null);
    insertRowWithTwoColumns(-1, 1);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(null, -1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE NOT(OR(%s > 0, %s < 0)) LIMIT 100", COLUMN1, COLUMN2, COLUMN1,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{-1, 1});
  }

  @Test
  public void testBaseColumnFilterOperatorGetNullBitmapIsNull()
      throws Exception {
    initializeRows();
    insertRow(false);
    insertRow(true);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.BOOLEAN).build();
    setUpSegments(TABLE_CONFIG_WITH_INVERTED_INDEX_COLUMN, schema);
    String query = String.format("SELECT * FROM testTable WHERE NOT(%s = false)", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{true});
  }

  @Test
  public void testAdditionExpressionFilterOperator()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(Integer.MIN_VALUE);
    insertRow(1);
    insertRow(-1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT %s FROM testTable WHERE add(%s, 0) < 0", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES * 2);
  }

  @Test
  public void testAdditionExpressionFilterOperatorInsideNotFilterOperator()
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(Integer.MIN_VALUE);
    insertRow(1);
    insertRow(-1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT %s FROM testTable WHERE NOT(add(%s, 0) > 0)", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES * 2);
  }

  @Test
  public void testGreatestExpressionFilterOperator()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(Integer.MIN_VALUE, Integer.MIN_VALUE);
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(-1, -1);
    insertRowWithTwoColumns(-1, null);
    insertRowWithTwoColumns(null, -1);
    insertRowWithTwoColumns(1, 1);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE GREATEST(%s, %s) < 0 LIMIT 100", COLUMN1, COLUMN2, COLUMN1,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES * 2);
  }

  @Test
  public void testExpressionFilterOperatorResultIsInSecondProjectionBlock()
      throws Exception {
    initializeRows();
    for (int i = 0; i < DocIdSetPlanNode.MAX_DOC_PER_CALL; i++) {
      insertRowWithTwoColumns(null, i);
    }
    insertRowWithTwoColumns(1, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE add(%s, 0) > 0 LIMIT 10", COLUMN1, COLUMN2, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{1, DocIdSetPlanNode.MAX_DOC_PER_CALL});
  }

  @Test
  public void testExpressionFilterOperatorApplyAndForGetFalses()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(-1, 1);
    insertRowWithTwoColumns(Integer.MIN_VALUE, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s FROM testTable WHERE NOT(add(%s, 0) > 0) AND %s IS NULL", COLUMN1, COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{Integer.MIN_VALUE});
  }

  @Test
  public void testExpressionFilterOperatorNotFilterOnMultiValue()
      throws Exception {
    initializeRows();
    insertRow(new Integer[]{1, 2, 3});
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT * FROM testTable WHERE NOT(VALUEIN(%s, 2, 3) > 2) LIMIT 100", COLUMN1);

    List<Object[]> rows = getRows(query, null);
    assertEquals(rows.size(), 0);
  }

  @Test
  public void testExpressionFilterOperatoIsNullPredicate()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(1, 2);
    insertRowWithTwoColumns(-1, 3);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE ADD(%s, 0) IS NULL LIMIT 100", COLUMN1, COLUMN2, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{null, 1});
  }

  @Test
  public void testExpressionFilterOperatorIsNotNullPredicate()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(1, 3);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE ADD(%s, 0) IS NOT NULL LIMIT 100", COLUMN1, COLUMN2, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{1, 3});
  }

  @Test
  public void testExpressionFilterOperatorIsNullPredicateInsideNotFilterOperator()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(1, 3);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE NOT(ADD(%s, 0) IS NULL) LIMIT 100", COLUMN1, COLUMN2,
            COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{1, 3});
  }

  @Test
  public void testExpressionFilterOperatorIsNotNullPredicateInsideNotFilterOperator()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 1);
    insertRowWithTwoColumns(2, 3);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE NOT(ADD(%s, 0) IS NOT NULL) LIMIT 100", COLUMN1, COLUMN2,
            COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{null, 1});
  }

  @Test
  public void testExpressionFilterOperatorApplyIsNullPredicateToNotOfColumn()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(true, 1);
    insertRowWithTwoColumns(null, 2);
    insertRowWithTwoColumns(false, 3);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE (NOT %s) IS NULL LIMIT 100", COLUMN1, COLUMN2, COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{null, 2});
  }

  @Test
  public void testExpressionFilterOperatorApplyAndForGetNulls()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(Integer.MIN_VALUE, null);
    insertRowWithTwoColumns(1, null);
    insertRowWithTwoColumns(-1, 1);
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT %s, %s FROM testTable WHERE (add(%s, 0) IS NULL) AND (%s IS NULL)", COLUMN1, COLUMN2,
            COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{null, null});
  }

  @Test
  public void testExpressionFilterOperatorOnMultiValue()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(new Integer[]{1, 2, 3}, 1);
    insertRowWithTwoColumns(new Integer[]{2, 3, 4}, null);
    Schema schema = schemaBuilder()
        .addMultiValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT * FROM testTable WHERE (VALUEIN(%s, 2, 3) IN (2, 3)) AND (%s = 1)", COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{new Integer[]{1, 2, 3}, 1});
  }

  @Test
  public void testExpressionFilterOperatorMultiValueIsNull()
      throws Exception {
    initializeRows();
    insertRow(new Integer[]{1, 2, 3});
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT * FROM testTable WHERE (VALUEIN(%s, 2, 3) IS NULL)", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 0);
  }

  @Test
  public void testExpressionFilterOperatorMultiValueIsNotNull()
      throws Exception {
    initializeRows();
    insertRow(new Integer[]{1, 2, 3});
    Schema schema = schemaBuilder().addMultiValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT * FROM testTable WHERE (VALUEIN(%s, 2, 3) IS NOT NULL)", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{new Integer[]{1, 2, 3}});
  }

  @Test
  public void testScalarFunctionStringNullLiteral()
      throws Exception {
    initializeRows();
    insertRow("abc");
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.STRING).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT STARTSWITH(%s, NULL) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 2 * NUM_OF_SEGMENT_COPIES);
    for (int i = 0; i < 2 * NUM_OF_SEGMENT_COPIES; i++) {
      assertEquals(rows.get(i), new Object[]{null});
    }
  }

  @Test
  public void testScalarFunctionIntNullLiteral()
      throws Exception {
    initializeRows();
    insertRow(1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.INT).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT between(%s, NULL, 2) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), NUM_OF_SEGMENT_COPIES);
    assertEquals(rows.get(0), new Object[]{null});
  }

  @Test(dataProvider = "NumberTypes")
  public void testStddevPop(DataType dataType)
      throws Exception {
    initializeRows();
    insertRow(null);
    insertRow(1);
    insertRow(2);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, dataType).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT STDDEV_POP(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 0.5);
  }

  /// A covariance pairs a value from each column, so a row counts only when both are present.
  @Test
  public void testCovarPopSkipsRowsWhereEitherColumnIsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(1, 10);
    insertRowWithTwoColumns(null, 20);
    insertRowWithTwoColumns(3, null);
    insertRowWithTwoColumns(4, 40);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT COVAR_POP(%s, %s) FROM testTable", COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    // Only rows 0 and 3 contribute: mean(xy) - mean(x)mean(y) = 85 - 2.5 * 25
    assertEquals(rows.get(0)[0], 22.5);
  }

  @Test
  public void testCovarPopOverAllNullInputIsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 10);
    insertRowWithTwoColumns(null, 20);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT COVAR_POP(%s, %s) FROM testTable", COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertNull(rows.get(0)[0]);
  }

  /// With the option off, null rows are read as the column default and folded in, as they always have been.
  @Test
  public void testCovarPopFoldsNullRowsWhenOptionDisabled()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, 10);
    insertRowWithTwoColumns(null, 20);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.INT)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT COVAR_POP(%s, %s) FROM testTable", COLUMN1, COLUMN2);

    List<Object[]> rows = getRows(query, null);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], 0.0);
  }

  @Test(dataProvider = "NumberTypes")
  public void testGroupByStddevPop(DataType dataType)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, "key");
    insertRowWithTwoColumns(1, "key");
    insertRowWithTwoColumns(2, "key");
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addSingleValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT STDDEV_POP(%s), %s FROM testTable GROUP BY %s", COLUMN1, COLUMN2, COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0), new Object[]{0.5, "key"});
  }

  @Test(dataProvider = "NumberTypes")
  public void testGroupByMvStddevPop(DataType dataType)
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, new String[]{"key1", "key2"});
    insertRowWithTwoColumns(1, new String[]{"key1", "key2"});
    insertRowWithTwoColumns(2, new String[]{"key1"});
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, dataType)
        .addMultiValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT STDDEV_POP(%s), %s FROM testTable GROUP BY %s ORDER BY %s", COLUMN1, COLUMN2, COLUMN2,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 2);
    assertEquals(rows.get(0), new Object[]{0.5, "key1"});
    assertEquals(rows.get(1), new Object[]{0.0, "key2"});
  }

  @Test
  public void testAllNullGroupByStddevPopReturnsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, "key1");
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.INT)
        .addSingleValueDimension(COLUMN2, DataType.STRING)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query =
        String.format("SELECT STDDEV_POP(%s), %s FROM testTable GROUP BY %s ORDER BY %s", COLUMN1, COLUMN2, COLUMN2,
            COLUMN2);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertNull(rows.get(0)[0]);
  }

  @Test
  public void testAllNullStddevPopReturnsNull()
      throws Exception {
    initializeRows();
    insertRow(null);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.DOUBLE).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT STDDEV_POP(%s) FROM testTable", COLUMN1);

    List<Object[]> rows = getRows(query);
    assertEquals(rows.size(), 1);
    assertNull(rows.get(0)[0]);
  }

  @Test
  public void testNoMatchingRowNullHandlingDisabledStddevPopReturnsNull()
      throws Exception {
    initializeRows();
    insertRow(1);
    Schema schema = schemaBuilder().addSingleValueDimension(COLUMN1, DataType.DOUBLE).build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT STDDEV_POP(%s) FROM testTable WHERE %s != 1", COLUMN1, COLUMN1);

    List<Object[]> rows = getRows(query, null);
    assertEquals(rows.size(), 1);
    assertEquals(rows.get(0)[0], Double.NEGATIVE_INFINITY);
  }

  @Test
  public void testTrueAndNullReturnsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(true, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.BOOLEAN)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT AND(%s, %s) FROM testTable LIMIT 1", COLUMN1, COLUMN2);

    assertNull(getRows(query).get(0)[0]);
  }

  @Test
  public void testFalseAndNullReturnsFalse()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(false, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.BOOLEAN)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT AND(%s, %s) FROM testTable LIMIT 1", COLUMN1, COLUMN2);

    assertFalse((boolean) getRows(query).get(0)[0]);
  }

  @Test
  public void testNullAndNullReturnsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.BOOLEAN)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT AND(%s, %s) FROM testTable LIMIT 1", COLUMN1, COLUMN2);

    assertNull(getRows(query).get(0)[0]);
  }

  @Test
  public void testTrueOrNullReturnsTrue()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(true, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.BOOLEAN)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT OR(%s, %s) FROM testTable LIMIT 1", COLUMN1, COLUMN2);

    assertTrue((boolean) getRows(query).get(0)[0]);
  }

  @Test
  public void testFalseOrNullReturnsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(false, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.BOOLEAN)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT OR(%s, %s) FROM testTable LIMIT 1", COLUMN1, COLUMN2);

    assertNull(getRows(query).get(0)[0]);
  }

  @Test
  public void testNullOrNullReturnsNull()
      throws Exception {
    initializeRows();
    insertRowWithTwoColumns(null, null);
    Schema schema = schemaBuilder()
        .addSingleValueDimension(COLUMN1, DataType.BOOLEAN)
        .addSingleValueDimension(COLUMN2, DataType.BOOLEAN)
        .build();
    setUpSegments(TABLE_CONFIG, schema);
    String query = String.format("SELECT OR(%s, %s) FROM testTable LIMIT 1", COLUMN1, COLUMN2);

    assertNull(getRows(query).get(0)[0]);
  }

  @AfterMethod
  public void destroySegments() {
    _indexSegment.destroy();
  }
}
