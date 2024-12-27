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
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTableRows;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * TODO: Find a good way to consolidate this with ForwardIndexDisabledMultiValueQueriesTest
 * The <code>ForwardIndexDisabledMultiValueQueriesWithReloadTest</code> class sets up the index segment for the
 * no forward index multi-value queries test with reload.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValue, FwdIndexDisabled: S1, S2
 *   <li>column1, METRIC, INT, 51594, F, F, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, T, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, T, T, T, T</li>
 *   <li>column7, DIMENSION, INT, 359, F, F, T, F, F</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F, F, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F, F, F</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F, F, F</li>
 * </ul>
 */
public class ForwardIndexDisabledMultiValueQueriesWithReloadTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), ForwardIndexDisabledMultiValueQueriesWithReloadTest.class.getSimpleName());
  private static final String AVRO_DATA = "data" + File.separator + "test_data-mv.avro";
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addMetric("column1", DataType.INT)
      .addMetric("column2", DataType.INT)
      .addSingleValueDimension("column3", DataType.STRING)
      .addSingleValueDimension("column5", DataType.STRING)
      .addMultiValueDimension("column6", DataType.INT)
      .addMultiValueDimension("column7", DataType.INT)
      .addSingleValueDimension("column8", DataType.INT)
      .addMetric("column9", DataType.INT)
      .addMetric("column10", DataType.INT)
      .addDateTime("daysSinceEpoch", DataType.INT, "EPOCH|DAYS", "1:DAYS")
      .build();

  // Hard-coded query filter.
  protected static final String FILTER = " WHERE column1 > 100000000"
      + " AND column2 BETWEEN 20000000 AND 1000000000"
      + " AND column3 <> 'w'"
      + " AND (column6 < 500000 OR column7 NOT IN (225, 407))"
      + " AND daysSinceEpoch = 1756015683";
  //@formatter:on

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments.
  private List<IndexSegment> _indexSegments;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    TableConfig tableConfig =
        createTableConfig(List.of("column5", "column7"), List.of("column3", "column6", "column8", "column9"),
            List.of("column6"));

    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String avroFile = resource.getFile();

    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    generatorConfig.setInputFilePath(avroFile);
    generatorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    generatorConfig.setSegmentName(SEGMENT_NAME);
    generatorConfig.setSkipTimeValueCheck(true);
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(generatorConfig);
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME),
        new IndexLoadingConfig(tableConfig, SCHEMA));
    Map<String, ColumnMetadata> columnMetadataMap = segment.getSegmentMetadata().getColumnMetadataMap();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      String column = entry.getKey();
      ColumnMetadata metadata = entry.getValue();
      if (column.equals("column6")) {
        assertTrue(metadata.hasDictionary());
        assertFalse(metadata.isSingleValue());
        assertNull(segment.getForwardIndex(column));
      } else {
        assertNotNull(segment.getForwardIndex(column));
      }
    }

    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  private TableConfig createTableConfig(List<String> noDictionaryColumns, List<String> invertedIndexColumns,
      List<String> forwardIndexDisabledColumns) {
    List<FieldConfig> fieldConfigs = new ArrayList<>(forwardIndexDisabledColumns.size());
    for (String column : forwardIndexDisabledColumns) {
      fieldConfigs.add(new FieldConfig(column, FieldConfig.EncodingType.DICTIONARY, List.of(), null,
          Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true")));
    }
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
        .setNoDictionaryColumns(noDictionaryColumns).setInvertedIndexColumns(invertedIndexColumns)
        .setCreateInvertedIndexDuringSegmentGeneration(true).setFieldConfigList(fieldConfigs).build();
  }

  @AfterMethod
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @Override
  protected String getFilter() {
    return FILTER;
  }

  @Override
  protected IndexSegment getIndexSegment() {
    return _indexSegment;
  }

  @Override
  protected List<IndexSegment> getIndexSegments() {
    return _indexSegments;
  }

  @Test
  public void testSelectQueriesWithReload()
      throws Exception {
    // Selection query without filters including column7
    // This is just a sanity check to ensure the query works when forward index is enabled
    String query = "SELECT column1, column5, column7 FROM testTable WHERE column7 != 201 ORDER BY column1";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    ResultTableRows resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 399_896L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_976L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 536_360L);
    DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column7"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT_ARRAY});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    List<Object[]> resultRows = resultTableRows.getRows();
    int previousColumn1 = Integer.MIN_VALUE;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      // Column 1
      assertTrue((Integer) resultRow[0] >= previousColumn1);
      previousColumn1 = (Integer) resultRow[0];
    }
    Object[] firstRow = resultRows.get(0);
    // Column 5
    assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");

    // Disable forward index for column7
    disableForwardIndexForSomeColumns();

    // Run the same query and validate that an exception is thrown since column7 has forward index disabled
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() != null
        && brokerResponseNative.getExceptions().size() > 0);

    // Run a query which uses column7 in the where clause and not in the select list. This should work
    query = "SELECT column1, column5 FROM testTable WHERE column7 IN (201, 2147483647) ORDER BY column1";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 199_860L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 199_900L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    dataSchema = new DataSchema(new String[]{"column1", "column5"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    resultRows = resultTableRows.getRows();
    previousColumn1 = Integer.MIN_VALUE;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
      assertTrue((Integer) resultRow[0] >= previousColumn1);
      previousColumn1 = (Integer) resultRow[0];
    }
    firstRow = resultRows.get(0);
    // Column 5
    assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");

    // Transform function on a filter clause for forwardIndexDisabled column in transform
    query = "SELECT column1, column10 from testTable WHERE ARRAYLENGTH(column7) = 2";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() != null
        && brokerResponseNative.getExceptions().size() > 0);

    // Re-enable forward index for column7 and column6
    reenableForwardIndexForSomeColumns();

    // Selection query without filters including column7
    query = "SELECT column1, column5, column7, column6 FROM testTable WHERE column7 != 201 ORDER BY column1";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 399_896L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_016L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 536_360L);
    dataSchema = new DataSchema(new String[]{"column1", "column5", "column7", "column6"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT_ARRAY});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    resultRows = resultTableRows.getRows();
    previousColumn1 = Integer.MIN_VALUE;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 4);
      // Column 1
      assertTrue((Integer) resultRow[0] >= previousColumn1);
      previousColumn1 = (Integer) resultRow[0];
    }
    firstRow = resultRows.get(0);
    // Column 5
    assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");

    // Transform function on a filter clause for forwardIndexDisabled column in transform
    query = "SELECT column1, column10 from testTable WHERE ARRAYLENGTH(column7) = 2 AND ARRAYLENGTH(column6) = 2 "
        + "ORDER BY column1";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 5388L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 5428L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 799_056L);
    dataSchema = new DataSchema(new String[]{"column1", "column10"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    resultRows = resultTableRows.getRows();
    previousColumn1 = Integer.MIN_VALUE;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
      // Column 1
      assertTrue((Integer) resultRow[0] >= previousColumn1);
      previousColumn1 = (Integer) resultRow[0];
    }
  }

  @Test
  public void testSelectAllResultsQueryWithReload()
      throws Exception {
    // Select query with order by on column9 with limit == totalDocs
    String query = "SELECT column7 FROM testTable ORDER BY column1 LIMIT 400000";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    ResultTableRows resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 400_000);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 800_000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    DataSchema dataSchema = new DataSchema(new String[]{"column7"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT_ARRAY});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    List<Object[]> resultRowsBeforeDisabling = resultTableRows.getRows();
    for (Object[] resultRow : resultRowsBeforeDisabling) {
      assertEquals(resultRow.length, 1);
    }

    // Disable forward index for column7
    disableForwardIndexForSomeColumns();

    // Run the same query and validate that an exception is thrown since we are running select query on forward index
    // disabled column
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() != null
        && brokerResponseNative.getExceptions().size() > 0);

    // Re-enable forward index for column7 and column6
    reenableForwardIndexForSomeColumns();

    // The first query should work now
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 400_000);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 800_000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    dataSchema = new DataSchema(new String[]{"column7"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT_ARRAY});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    List<Object[]> resultRowsAfterReenabling = resultTableRows.getRows();
    // Validate that the result row size before disabling the forward index matches the result row size after
    // re-enabling the forward index
    assertEquals(resultRowsAfterReenabling.size(), resultRowsBeforeDisabling.size());
    // Validate the first 10 rows
    for (int i = 0; i < 10; i++) {
      Object[] resultRow = resultRowsAfterReenabling.get(i);
      assertEquals(resultRow.length, 1);
      int[] rowValuesAfterReenabling = (int[]) resultRow[0];
      int[] rowValuesBeforeDisabling = (int[]) resultRowsBeforeDisabling.get(i)[0];
      assertEquals(rowValuesAfterReenabling.length, rowValuesBeforeDisabling.length);
      // Validate that the value of result row matches the value at this index before forward index was disabled
      // Since ordering cannot be guaranteed for multi-value rows, validate all entries are present
      Set<Integer> rowValuesSetAfterReenabling = new HashSet<>();
      Set<Integer> rowValuesSetBeforeDisabling = new HashSet<>();
      for (int j = 0; j < rowValuesAfterReenabling.length; j++) {
        rowValuesSetAfterReenabling.add(rowValuesAfterReenabling[j]);
        rowValuesSetBeforeDisabling.add(rowValuesBeforeDisabling[j]);
      }
      assertEquals(rowValuesSetAfterReenabling, rowValuesSetBeforeDisabling);
    }
  }

  @Test
  public void testSelectWithDistinctQueriesWithReload()
      throws Exception {
    // Distinct query without filters including column7
    // This is just a sanity check to ensure the query works when forward index is enabled
    String query = "SELECT DISTINCT column1, column7, column9 FROM testTable ORDER BY column1 LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    ResultTableRows resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1200000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    DataSchema dataSchema = new DataSchema(new String[]{"column1", "column7", "column9"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.INT});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    List<Object[]> resultRows = resultTableRows.getRows();
    int previousColumn1 = Integer.MIN_VALUE;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertTrue(previousColumn1 <= (int) resultRow[0]);
      previousColumn1 = (int) resultRow[0];
    }

    // Disable forward index for column7
    disableForwardIndexForSomeColumns();

    // Run the same query and validate that an exception is thrown since column7 has forward index disabled
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in distinct");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Forward index disabled for column: column7, cannot create DataFetcher!");
    }

    // Re-enable forward index for column7 and column6
    reenableForwardIndexForSomeColumns();

    // Distinct query without filters including column7
    query = "SELECT DISTINCT column1, column7, column9, column6 FROM testTable ORDER BY column1 LIMIT 10";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1600000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    dataSchema = new DataSchema(new String[]{"column1", "column7", "column9", "column6"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
    assertEquals(resultTableRows.getDataSchema(), dataSchema);
    resultRows = resultTableRows.getRows();
    previousColumn1 = Integer.MIN_VALUE;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 4);
      assertTrue(previousColumn1 <= (int) resultRow[0]);
      previousColumn1 = (int) resultRow[0];
    }
  }

  @Test
  public void testSelectWithGroupByOrderByQueriesWithReload()
      throws Exception {
    // Select non-forwardIndexDisabled columns with group by order by
    // This is just a sanity check to ensure the query works when forward index is enabled
    String query = "SELECT column1, column7 FROM testTable GROUP BY column1, column7 ORDER BY column1, column7 "
        + " LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    ResultTableRows resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 800000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"column1", "column7"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT}));
    List<Object[]> resultRows = resultTableRows.getRows();
    int previousVal = -1;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
      assertTrue((int) resultRow[0] >= previousVal);
      previousVal = (int) resultRow[0];
    }

    // Disable forward index for column7
    disableForwardIndexForSomeColumns();

    // Run the same query and validate that an exception is thrown since column7 has forward index disabled
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in group by order by");
    } catch (IllegalStateException e) {
      assertEquals(e.getMessage(), "Forward index disabled for column: column7, cannot create DataFetcher!");
    }

    // Select forwardIndexDisabled columns using transform with group by order by
    query = "SELECT ARRAYLENGTH(column7) FROM testTable GROUP BY ARRAYLENGTH(column7) ORDER BY "
        + "ARRAYLENGTH(column7) LIMIT 10";
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in group by order by");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }

    // Re-enable forward index for column7 and column6
    reenableForwardIndexForSomeColumns();

    // Select non-forwardIndexDisabled columns with group by order by
    query = "SELECT column1, column7, column6 FROM testTable GROUP BY column1, column7, column6 ORDER BY column1, "
        + "column7, column6 LIMIT 10";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1200000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"column1", "column7", "column6"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
            DataSchema.ColumnDataType.INT}));
    resultRows = resultTableRows.getRows();
    previousVal = -1;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertTrue((int) resultRow[0] >= previousVal);
      previousVal = (int) resultRow[0];
    }

    // Select forwardIndexDisabled columns using transform with group by order by
    query = "SELECT ARRAYLENGTH(column7) FROM testTable GROUP BY ARRAYLENGTH(column7) ORDER BY "
        + "ARRAYLENGTH(column7) LIMIT 10";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"arraylength(column7)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT}));
    resultRows = resultTableRows.getRows();
    previousVal = -1;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
      assertTrue((int) resultRow[0] >= previousVal);
      previousVal = (int) resultRow[0];
    }
  }

  @Test
  public void testSelectWithAggregationQueriesWithReload()
      throws Exception {
    // This is just a sanity check to ensure the query works when forward index is enabled
    String query = "SELECT MAX(ARRAYLENGTH(column7)) from testTable LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    ResultTableRows resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"max(arraylength(column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> resultRows = resultTableRows.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
      assertEquals(resultRow[0], 24.0);
    }

    // Not allowed aggregation functions on non-forwardIndexDisabled columns
    // This is just a sanity check to ensure the query works when forward index is enabled
    query = "SELECT summv(column7), avgmv(column7) from testTable";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"summv(column7)", "avgmv(column7)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTableRows.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 2);
      assertEquals(resultRow[0], 4.28972873682684E14);
      assertEquals(resultRow[1], 7.997853562582668E8);
    }

    // Disable forward index for column7
    disableForwardIndexForSomeColumns();

    // The same query without forward index should fail
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }

    // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
    // column and group by order by on non-forwardIndexDisabled column
    query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 2147483647 GROUP BY "
        + "column1 ORDER BY column1";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"column1", "max(column1)",
        "sum(column9)"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTableRows.getRows();
    int previousVal = -1;
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertTrue((int) resultRow[0] > previousVal);
      previousVal = (int) resultRow[0];
    }

    // Transform inside aggregation involving a forwardIndexDisabled column
    query = "SELECT MAX(ARRAYLENGTH(column7)) from testTable LIMIT 10";
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }

    // Re-enable forward index for column7 and column6
    reenableForwardIndexForSomeColumns();

    query = "SELECT MAX(ARRAYLENGTH(column7)) from testTable LIMIT 10";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"max(arraylength(column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTableRows.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 1);
      assertEquals(resultRow[0], 24.0);
    }

    // Not allowed aggregation functions on non-forwardIndexDisabled columns
    query = "SELECT summv(column7), avgmv(column7), summv(column6) from testTable";
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null
        || brokerResponseNative.getExceptions().size() == 0);
    resultTableRows = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 800_000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertEquals(resultTableRows.getDataSchema(), new DataSchema(new String[]{"summv(column7)", "avgmv(column7)",
        "summv(column6)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
            DataSchema.ColumnDataType.DOUBLE}));
    resultRows = resultTableRows.getRows();
    for (Object[] resultRow : resultRows) {
      assertEquals(resultRow.length, 3);
      assertEquals(resultRow[0], 4.28972873682684E14);
      assertEquals(resultRow[1], 7.997853562582668E8);
      assertEquals(resultRow[2], 4.8432460181028E14);
    }
  }

  private void disableForwardIndexForSomeColumns()
      throws Exception {
    // Now disable forward index for column7 in the table config
    TableConfig tableConfig =
        createTableConfig(List.of("column5"), List.of("column3", "column6", "column7", "column8", "column9"),
            List.of("column6", "column7"));
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

    // Reload the segments to pick up the new configs
    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    ImmutableSegment segment = reloadSegment(indexDir, indexLoadingConfig, SCHEMA);
    _indexSegment.destroy();
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);

    assertNull(segment.getForwardIndex("column7"));
    assertNotNull(segment.getInvertedIndex("column7"));
    assertNotNull(segment.getDictionary("column7"));
  }

  private void reenableForwardIndexForSomeColumns()
      throws Exception {
    // Now re-enable forward index for column7 in the table config
    // Also re-enable forward index for column6
    TableConfig tableConfig =
        createTableConfig(List.of("column5", "column7"), List.of("column3", "column6", "column8", "column9"),
            List.of());
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

    // Reload the segments to pick up the new configs
    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    ImmutableSegment segment = reloadSegment(indexDir, indexLoadingConfig, SCHEMA);
    _indexSegment.destroy();
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);

    assertNotNull(segment.getForwardIndex("column7"));
    assertNull(segment.getInvertedIndex("column7"));
    assertNull(segment.getDictionary("column7"));
    assertNotNull(segment.getForwardIndex("column6"));
    assertNotNull(segment.getInvertedIndex("column6"));
    assertNotNull(segment.getDictionary("column6"));
  }
}
