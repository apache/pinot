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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * The <code>ForwardIndexDisabledMultiValueQueriesTest</code> class sets up the index segment for the no forward
 * index multi-value queries test.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValue, FwdIndexDisabled: S1, S2
 *   <li>column1, METRIC, INT, 51594, F, F, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, T, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, T, T, T, T</li>
 *   <li>column7, DIMENSION, INT, 359, F, T, T, T, F</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F, F, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F, F, F</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F, F, F</li>
 * </ul>
 */
public class ForwardIndexDisabledMultiValueQueriesTest extends BaseQueriesTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-mv.avro";
  private static final String SEGMENT_NAME_1 = "testTable_1756015688_1756015688";
  private static final String SEGMENT_NAME_2 = "testTable_1756015689_1756015689";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(),
      "ForwardIndexDisabledMultiValueQueriesTest");

  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";

  // Hard-coded query filter.
  protected static final String FILTER = " WHERE column1 > 100000000"
      + " AND column2 BETWEEN 20000000 AND 1000000000"
      + " AND column3 <> 'w'"
      + " AND (column6 < 500000 OR column7 NOT IN (225, 407))"
      + " AND daysSinceEpoch = 1756015683";

  private IndexSegment _indexSegment;
  // Contains 2 identical index segments.
  private List<IndexSegment> _indexSegments;

  private TableConfig _tableConfig;
  private List<String> _invertedIndexColumns;
  private List<String> _forwardIndexDisabledColumns;

  @BeforeMethod
  public void buildSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column2", FieldSpec.DataType.INT).addSingleValueDimension("column3", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addMultiValueDimension("column6", FieldSpec.DataType.INT)
        .addMultiValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column8", FieldSpec.DataType.INT).addMetric("column9", FieldSpec.DataType.INT)
        .addMetric("column10", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "daysSinceEpoch"), null).build();

    createSegment(filePath, SEGMENT_NAME_1, schema);
    createSegment(filePath, SEGMENT_NAME_2, schema);

    ImmutableSegment immutableSegment1 = loadSegmentWithMetadataChecks(SEGMENT_NAME_1);
    ImmutableSegment immutableSegment2 = loadSegmentWithMetadataChecks(SEGMENT_NAME_2);

    _indexSegment = immutableSegment1;
    _indexSegments = Arrays.asList(immutableSegment1, immutableSegment2);
  }

  private void createSegment(String filePath, String segmentName, Schema schema)
      throws Exception {
    // Create field configs for the no forward index columns
    List<FieldConfig> fieldConfigList = new ArrayList<>();
    fieldConfigList.add(new FieldConfig("column6", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(), null,
        Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));
    if (segmentName.equals(SEGMENT_NAME_1)) {
      fieldConfigList.add(new FieldConfig("column7", FieldConfig.EncodingType.DICTIONARY, Collections.emptyList(),
          null, Collections.singletonMap(FieldConfig.FORWARD_INDEX_DISABLED, Boolean.TRUE.toString())));

      // Build table config based on segment 1 as it contains both columns under no forward index
      _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setNoDictionaryColumns(Arrays.asList("column5"))
          .setTableName("testTable").setTimeColumnName("daysSinceEpoch").setFieldConfigList(fieldConfigList).build();
    }

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);
    _invertedIndexColumns = Arrays.asList("column3", "column6", "column7", "column8", "column9");
    segmentGeneratorConfig.setIndexOn(StandardIndexes.inverted(), IndexConfig.ENABLED, _invertedIndexColumns);
    _forwardIndexDisabledColumns = new ArrayList<>(Arrays.asList("column6", "column7"));
    segmentGeneratorConfig.setIndexOn(StandardIndexes.forward(), ForwardIndexConfig.DISABLED,
        _forwardIndexDisabledColumns);
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGeneratorConfig.setSkipTimeValueCheck(true);

    // Build the index segment.
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();
  }

  private ImmutableSegment loadSegmentWithMetadataChecks(String segmentName)
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setTableConfig(_tableConfig);
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(_invertedIndexColumns));
    indexLoadingConfig.setForwardIndexDisabledColumns(new HashSet<>(_forwardIndexDisabledColumns));
    indexLoadingConfig.setReadMode(ReadMode.heap);

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName),
        indexLoadingConfig);

    Map<String, ColumnMetadata> columnMetadataMap1 = immutableSegment.getSegmentMetadata().getColumnMetadataMap();
    columnMetadataMap1.forEach((column, metadata) -> {
      if (column.equals("column6") || column.equals("column7")) {
        assertTrue(metadata.hasDictionary());
        assertFalse(metadata.isSingleValue());
        assertNull(immutableSegment.getForwardIndex(column));
      } else {
        assertNotNull(immutableSegment.getForwardIndex(column));
      }
    });

    return immutableSegment;
  }

  @AfterMethod
  public void deleteAndDestroySegment() {
    FileUtils.deleteQuietly(INDEX_DIR);
    _indexSegments.forEach((IndexSegment::destroy));
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
  public void testSelectStarQueries() {
    // Select * without any filters
    try {
      getBrokerResponse(SELECT_STAR_QUERY);
      Assert.fail("Select * query should fail since forwardIndexDisabled on a select column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }

    // Select * with filters
    try {
      getBrokerResponse(SELECT_STAR_QUERY + FILTER);
      Assert.fail("Select * query should fail since forwardIndexDisabled on a select column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("scan based filtering not supported!"));
    }
  }

  @Test
  public void testSelectQueries() {
    {
      // Selection query without filters including a column with forwardIndexDisabled enabled on both segments
      String query = "SELECT column1, column5, column6, column9, column10 FROM testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query without filters including a column with forwardIndexDisabled enabled on one segment
      String query = "SELECT column1, column5, column7, column9, column10 FROM testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query with filters including a column with forwardIndexDisabled enabled on both segments
      String query = "SELECT column1, column5, column6, column9, column10 FROM testTable WHERE column6 = 1001";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query with filters including a column with forwardIndexDisabled enabled on one segment
      String query = "SELECT column1, column5, column7, column9, column10 FROM testTable WHERE column7 = 2147483647";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query without filters and without columns with forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_120L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        // Column 1
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Transform function on a selection clause with a forwardIndexDisabled column in transform
      String query = "SELECT ARRAYLENGTH(column6) from testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a select column in transform");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Selection query with filters (not including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column1 > 100000000"
          + " AND column2 BETWEEN 20000000 AND 1000000000"
          + " AND column3 <> 'w'"
          + " AND daysSinceEpoch = 1756015683 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 62_700L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 62_820);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 304_120L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column6 = 1001 "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 8);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 8L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 32L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals(resultRow[0], 1849044734);
        assertEquals((String) resultRow[1], "AKXcXcIqsqOJFsdwxZ");
        assertEquals(resultRow[2], 674022574);
        assertEquals(resultRow[3], 674022574);
      }
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column7 != 201 "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 399_896L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_016L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column7 IN (201, 2147483647) "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 199_860L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 199_980L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "AKXcXcIqsqOJFsdwxZ");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column6 NOT IN "
          + "(1001, 2147483647) ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 174_552L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 174_672L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column10"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 5
      assertEquals((String) firstRow[1], "EOFxevm");
    }
    {
      // Query with literal only in SELECT
      String query = "SELECT 'marvin' from testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400_000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"'marvin'"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 1);
        assertEquals((String) resultRow[0], "marvin");
      }
    }
    {
      // Selection query with '<' filter on a forwardIndexDisabled column without range index available
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column6 < 2147483647 AND "
          + "column6 >= 1001 ORDER BY column1";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a range query column without range index");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("scan based filtering not supported!"));
      }
    }
    {
      // Selection query with '>=' filter on a forwardIndexDisabled column without range index available
      String query = "SELECT column1, column5, column9, column10 FROM testTable WHERE column7 > 201";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a range query column without range index");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("scan based filtering not supported!"));
      }
    }
    {
      // Select query with a filter on a column which doesn't have forwardIndexDisabled enabled
      String query = "SELECT column1, column5, column9 from testTable WHERE column9 < 3890167 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 48L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 128L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 400_000L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5", "column9"},
          new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT
          }));
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1Value = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 3);
        assertTrue((Integer) resultRow[0] >= previousColumn1Value);
        previousColumn1Value = (Integer) resultRow[0];
        assertEquals(resultRow[2], 3890166);
      }
    }
    {
      // Transform function on a filter clause for forwardIndexDisabled column in transform
      String query = "SELECT column1, column10 from testTable WHERE ARRAYLENGTH(column6) = 2";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() != null
          && brokerResponseNative.getProcessingExceptions().size() > 0);
    }
  }

  @Test
  public void testSelectWithDistinctQueries() {
    // Select a mix of forwardIndexDisabled and non-forwardIndexDisabled columns with distinct
    String query = "SELECT DISTINCT column1, column6, column9 FROM testTable LIMIT 10";
    try {
      getBrokerResponse(query);
      Assert.fail("Query should fail since forwardIndexDisabled on a column in select distinct");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Forward index disabled for column:")
          && e.getMessage().contains("cannot create DataFetcher!"));
    }
  }

  @Test
  public void testSelectWithGroupByOrderByQueries() {
    {
      // Select a mix of forwardIndexDisabled and non-forwardIndexDisabled columns with group by order by
      String query = "SELECT column1, column6 FROM testTable GROUP BY column1, column6 ORDER BY column1, column6 "
          + " LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Select forwardIndexDisabled columns with group by order by
      String query = "SELECT column7, column6 FROM testTable GROUP BY column7, column6 ORDER BY column7, column6 "
          + " LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Select non-forwardIndexDisabled columns with group by order by
      String query = "SELECT column1, column5 FROM testTable GROUP BY column1, column5 ORDER BY column1, column5 "
          + " LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 800000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousVal = -1;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertTrue((int) resultRow[0] > previousVal);
        previousVal = (int) resultRow[0];
      }
    }
    {
      // Select forwardIndexDisabled columns using transform with group by order by
      String query = "SELECT ARRAYLENGTH(column6) FROM testTable GROUP BY ARRAYLENGTH(column6) ORDER BY "
          + "ARRAYLENGTH(column6) LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Test a select with a VALUEIN transform function with group by
      String query = "SELECT VALUEIN(column6, '1001') from testTable WHERE column6 IN (1001) GROUP BY "
          + "VALUEIN(column6, '1001') LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in select group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
  }

  @Test
  public void testSelectWithAggregationQueries() {
    {
      // Allowed aggregation functions on forwardIndexDisabled columns
      String query = "SELECT maxmv(column7), minmv(column6), minmaxrangemv(column6), distinctcountmv(column7), "
          + "distinctcounthllmv(column6), distinctcountrawhllmv(column7) from testTable";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"maxmv(column7)", "minmv(column6)",
          "minmaxrangemv(column6)", "distinctcountmv(column7)", "distinctcounthllmv(column6)",
          "distinctcountrawhllmv(column7)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
              DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.LONG,
              DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 6);
        assertEquals(resultRow[0], 2.147483647E9);
        assertEquals(resultRow[1], 1001.0);
        assertEquals(resultRow[2], 2.147482646E9);
        assertEquals(resultRow[3], 359);
        assertEquals(resultRow[4], 20039L);
      }
    }
    {
      // Not allowed aggregation functions on forwardIndexDisabled columns
      String query = "SELECT summv(column7), avgmv(column6) from testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with group by on non-forwardIndexDisabled
      // column
      String query = "SELECT column1, maxmv(column6) from testTable GROUP BY column1";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with group by order by on
      // non-forwardIndexDisabled column
      String query = "SELECT column1, maxmv(column6) from testTable GROUP BY column1 ORDER BY column1";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with group by on non-forwardIndexDisabled
      // column but order by on allowed aggregation function on forwardIndexDisabled column
      String query = "SELECT column1, max(column9) from testTable GROUP BY column1 ORDER BY minmv(column6)";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with a filter - results in trying to scan which
      // fails
      String query = "SELECT maxmv(column7), minmv(column6) from testTable WHERE column7 = 201";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on forwardIndexDisabled columns with a filter - results in trying to scan which
      // fails
      String query = "SELECT max(column1), minmv(column6) from testTable WHERE column1 > 15935";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column
      String query = "SELECT max(column1), sum(column9) from testTable WHERE column7 = 2147483647";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column1)", "sum(column9)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertEquals(resultRow[0], 2.147313491E9);
        assertEquals(resultRow[1], 1.38051889779548E14);
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column and group by order by on non-forwardIndexDisabled column
      String query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 2147483647 GROUP BY "
          + "column1 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "max(column1)", "sum(column9)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE,
              DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousVal = -1;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 3);
        assertTrue((int) resultRow[0] > previousVal);
        previousVal = (int) resultRow[0];
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column and group by on non-forwardIndexDisabled column order by on forwardIndexDisabled aggregation column
      String query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 201 GROUP BY "
          + "column1 ORDER BY minmv(column6)";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column
      String query = "SELECT MAX(ARRAYLENGTH(column6)) from testTable LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column with group by
      String query = "SELECT column1, MAX(ARRAYLENGTH(column6)) from testTable GROUP BY column1 LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column with group by order by
      String query = "SELECT column1, MAX(ARRAYLENGTH(column6)) from testTable GROUP BY column1 ORDER BY column1 "
          + "DESC LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
  }
}
