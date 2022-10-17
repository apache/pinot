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
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * The <code>ForwardIndexDisabledSingleValueQueriesTest</code> class sets up the index segment for the no forward
 * index single-value queries test.
 * <p>There are totally 18 columns, 30000 records inside the original Avro file where 11 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, FwdIndexDisabled: S1, S2, HasRangeIndex
 *   <li>column1, METRIC, INT, 6582, F, F, F, F, F</li>
 *   <li>column3, METRIC, INT, 21910, F, F, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 1, T, F, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 608, F, T, T, T, T</li>
 *   <li>column7, DIMENSION, INT, 146, F, T, T, F, F</li>
 *   <li>column9, DIMENSION, INT, 1737, F, F, F, F, F</li>
 *   <li>column11, DIMENSION, STRING, 5, F, T, F, F, F</li>
 *   <li>column12, DIMENSION, STRING, 5, F, F, F, F, F</li>
 *   <li>column17, METRIC, INT, 24, F, T, F, F, F</li>
 *   <li>column18, METRIC, INT, 1440, F, T, F, F, F</li>
 *   <li>daysSinceEpoch, TIME, INT, 2, T, F, F, F, F</li>
 * </ul>
 */
public class ForwardIndexDisabledSingleValueQueriesTest extends BaseQueriesTest {
  private static final String AVRO_DATA = "data" + File.separator + "test_data-sv.avro";
  private static final String SEGMENT_NAME_1 = "testTable_126164076_167572857";
  private static final String SEGMENT_NAME_2 = "testTable_126164076_167572858";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(),
      "ForwardIndexDisabledSingleValueQueriesTest");

  private static final String SELECT_STAR_QUERY = "SELECT * FROM testTable";

  // Hard-coded query filter.
  private static final String FILTER = " WHERE column1 > 100000000"
      + " AND column3 BETWEEN 20000000 AND 1000000000"
      + " AND column5 = 'gFuH'"
      + " AND (column6 < 500000000 OR column11 NOT IN ('t', 'P'))"
      + " AND daysSinceEpoch = 126164076";

  private IndexSegment _indexSegment;
  // Contains 2 index segments, one with 2 columns with forward index disabled, and the other with just 1.
  private List<IndexSegment> _indexSegments;

  private TableConfig _tableConfig;
  private List<String> _invertedIndexColumns;
  private List<String> _forwardIndexDisabledColumns;

  @BeforeClass
  public void buildAndLoadSegment()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    // Get resource file path.
    URL resource = getClass().getClassLoader().getResource(AVRO_DATA);
    assertNotNull(resource);
    String filePath = resource.getFile();

    // Build the segment schema.
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testTable").addMetric("column1", FieldSpec.DataType.INT)
        .addMetric("column3", FieldSpec.DataType.INT).addSingleValueDimension("column5", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column6", FieldSpec.DataType.INT)
        .addSingleValueDimension("column7", FieldSpec.DataType.INT)
        .addSingleValueDimension("column9", FieldSpec.DataType.INT)
        .addSingleValueDimension("column11", FieldSpec.DataType.STRING)
        .addSingleValueDimension("column12", FieldSpec.DataType.STRING).addMetric("column17", FieldSpec.DataType.INT)
        .addMetric("column18", FieldSpec.DataType.INT)
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
      _tableConfig =
          new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").setTimeColumnName("daysSinceEpoch")
              .setFieldConfigList(fieldConfigList).setRangeIndexColumns(Arrays.asList("column6")).build();
    }

    // Create the segment generator config.
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(_tableConfig, schema);
    segmentGeneratorConfig.setInputFilePath(filePath);
    segmentGeneratorConfig.setTableName("testTable");
    segmentGeneratorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGeneratorConfig.setSegmentName(segmentName);
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGeneratorConfig.setSkipTimeValueCheck(true);
    _invertedIndexColumns = Arrays.asList("column6", "column7", "column11", "column17", "column18");
    segmentGeneratorConfig.setInvertedIndexCreationColumns(_invertedIndexColumns);

    _forwardIndexDisabledColumns = new ArrayList<>(Arrays.asList("column6", "column7"));
    segmentGeneratorConfig.setForwardIndexDisabledColumns(_forwardIndexDisabledColumns);
    segmentGeneratorConfig.setRangeIndexCreationColumns(Arrays.asList("column6"));

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
    indexLoadingConfig.setRangeIndexColumns(new HashSet<>(Arrays.asList("column6")));
    indexLoadingConfig.setReadMode(ReadMode.heap);

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(new File(INDEX_DIR, segmentName),
        indexLoadingConfig);

    Map<String, ColumnMetadata> columnMetadataMap1 = immutableSegment.getSegmentMetadata().getColumnMetadataMap();
    columnMetadataMap1.forEach((column, metadata) -> {
      if (column.equals("column6") || column.equals("column7")) {
        assertTrue(metadata.hasDictionary());
        assertTrue(metadata.isSingleValue());
        assertNull(immutableSegment.getForwardIndex(column));
      } else {
        assertNotNull(immutableSegment.getForwardIndex(column));
      }
    });

    return immutableSegment;
  }

  @AfterClass
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
          && e.getMessage().contains("cannot create DataFetcher!"));
    }
  }

  @Test
  public void testSelectQueries() {
    {
      // Selection query without filters including a column with forwardIndexDisabled enabled on both segments
      String query = "SELECT column1, column5, column6, column9, column11 FROM testTable";
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
      String query = "SELECT column1, column5, column7, column9, column11 FROM testTable";
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
      String query = "SELECT column1, column5, column6, column9, column11 FROM testTable WHERE column6 = 2147458029";
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
      String query = "SELECT column1, column5, column7, column9, column11 FROM testTable WHERE column7 = 675695";
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
      String query = "SELECT column1, column5, column9, column11 FROM testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120_120L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals((String) resultRow[1], "gFuH");
        // Column 1
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 11
      assertEquals((String) firstRow[3], "o");
    }
    {
      // Transform function on a selection clause with a forwardIndexDisabled column in transform
      String query = "SELECT CONCAT(column6, column9, '-') from testTable";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in transform");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform function on a selection clause without a forwardIndexDisabled column in transform
      String query = "SELECT CONCAT(column5, column9, '-') from testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120_080L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"concat(column5,column9,'-')"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 1);
        assertTrue(resultRow[0].toString().startsWith("gFuH-"));
      }
    }
    {
      // Selection query with filters (not including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column1 > 100000000"
        + " AND column3 BETWEEN 20000000 AND 1000000000"
        + " AND column5 = 'gFuH'"
        + " AND daysSinceEpoch = 126164076 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 42_368L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 42_488);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 192744L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals((String) resultRow[1], "gFuH");
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 11
      assertEquals((String) firstRow[3], "P");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column6 = 2147458029 "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 16L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 64L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals(resultRow[0], 635553468);
        assertEquals((String) resultRow[1], "gFuH");
        assertEquals(resultRow[2], 705242697);
        assertEquals((String) resultRow[3], "P");
      }
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column7 != 675695 "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 119_908L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120_028L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals((String) resultRow[1], "gFuH");
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 11
      assertEquals((String) firstRow[3], "o");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column7 IN (675695, 2137685743) "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 828L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 948L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals((String) resultRow[1], "gFuH");
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 11
      assertEquals((String) firstRow[3], "P");
    }
    {
      // Selection query with supported filters (including forwardIndexDisabled column) and without columns with
      // forwardIndexDisabled enabled on either segment
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column6 NOT IN "
          + "(1689277, 2147458029) ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 119_980L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120_100L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals((String) resultRow[1], "gFuH");
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 11
      assertEquals((String) firstRow[3], "o");
    }
    {
      // Query with literal only in SELECT
      String query = "SELECT 'marvin' from testTable ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120_000L);
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
      // Selection query with '<' filter on a forwardIndexDisabled column with range index available
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column6 < 2147458029 AND "
          + "column6 > 1699000 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 119_980L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 120_100L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      DataSchema dataSchema = new DataSchema(new String[]{"column1", "column5", "column9", "column11"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
      assertEquals(resultTable.getDataSchema(), dataSchema);
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 4);
        assertEquals((String) resultRow[1], "gFuH");
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }

      Object[] firstRow = resultRows.get(0);
      // Column 11
      assertEquals((String) firstRow[3], "o");
    }
    {
      // Selection query with '>=' filter on a forwardIndexDisabled column without range index available
      String query = "SELECT column1, column5, column9, column11 FROM testTable WHERE column7 >= 676000";
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
      String query = "SELECT column1, column5, column9 from testTable WHERE column9 < 50000 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 4);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 12L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5", "column9"},
          new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT
      }));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 3);
      }
    }
    {
      // Transform function on a filter clause for forwardIndexDisabled column in transform
      String query = "SELECT column1, column11 from testTable WHERE CONCAT(column6, column9, '-') = '1689277-11270'";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() != null
          && brokerResponseNative.getProcessingExceptions().size() > 0);
    }
    {
      // Transform function on a filter clause for a non-forwardIndexDisabled column in transform
      String query = "SELECT column1, column11 from testTable WHERE CONCAT(column5, column9, '-') = 'gFuH-11270' "
          + "ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 4);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 8L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column11"},
          new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
          }));
      List<Object[]> resultRows = resultTable.getRows();
      assertEquals(resultRows.size(), 4);
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertEquals(resultRow[0], 815409257);
        assertEquals(resultRow[1], "P");
      }
    }
    {
      // Transform function on a filter clause for forwardIndexDisabled column in transform
      String query = "SELECT column1, column11 from testTable WHERE CONCAT(ADD(column6, column1), column9, '-') = "
          + "'1689277-11270'";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() != null
          && brokerResponseNative.getProcessingExceptions().size() > 0);
    }
    {
      // Transform function on a filter clause for a non-forwardIndexDisabled column in transform
      String query = "SELECT column1, column11 from testTable WHERE CONCAT(column5, ADD(column9, column1), '-') = "
          + "'gFuH-2.96708164E8' ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 28L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 56L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 120000L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column11"},
          new DataSchema.ColumnDataType[]{
              DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
          }));
      List<Object[]> resultRows = resultTable.getRows();
      assertEquals(resultRows.size(), 10);
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertEquals(resultRow[0], 240528);
        assertEquals(resultRow[1], "o");
      }
    }
  }

  @Test
  public void testSelectWithDistinctQueries() {
    {
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
    {
      // Select non-forwardIndexDisabled columns with distinct
      String query = "SELECT DISTINCT column1, column5, column9 FROM testTable ORDER BY column1 LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 360_000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5", "column9"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
              DataSchema.ColumnDataType.INT}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousColumn1 = Integer.MIN_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 3);
        assertEquals(resultRow[1], "gFuH");
        assertTrue((Integer) resultRow[0] >= previousColumn1);
        previousColumn1 = (Integer) resultRow[0];
      }
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
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "column5"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousVal = -1;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertEquals(resultRow[1], "gFuH");
        assertTrue((int) resultRow[0] > previousVal);
        previousVal = (int) resultRow[0];
      }
    }
    {
      // Select forwardIndexDisabled columns using transform with group by order by
      String query = "SELECT CONCAT(column1, column6, '-') FROM testTable GROUP BY CONCAT(column1, column6, '-') "
          + "ORDER BY CONCAT(column1, column6, '-') LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a transformed column in group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Select non-forwardIndexDisabled columns using transform with group by order by
      String query = "SELECT CONCAT(column1, column5, '-') FROM testTable GROUP BY CONCAT(column1, column5, '-') "
          + "ORDER BY CONCAT(column1, column5, '-') LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"concat(column1,column5,'-')"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 1);
        assertTrue(resultRow[0].toString().endsWith("-gFuH"));
      }
    }
    {
      // Select forwardIndexDisabled columns using nested transform with group by order by
      String query = "SELECT CONCAT(ADD(column1, column6), column5, '-') FROM testTable GROUP BY "
          + "CONCAT(ADD(column1, column6), column5, '-') ORDER BY CONCAT(ADD(column1, column6), column5, '-') LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail for forwardIndexDisabled on a nested transformed column in group by order by");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Select non-forwardIndexDisabled columns using nested transform with group by order by
      String query = "SELECT CONCAT(ADD(column1, column9), column5, '-') FROM testTable GROUP BY "
          + "CONCAT(ADD(column1, column9), column5, '-') ORDER BY CONCAT(ADD(column1, column9), column5, '-') LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 360000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"concat(add(column1,column9),column5,'-')"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 1);
        assertTrue(resultRow[0].toString().endsWith("-gFuH"));
      }
    }
  }

  @Test
  public void testSelectWithAggregationQueries() {
    {
      // Allowed aggregation functions on forwardIndexDisabled columns
      String query = "SELECT max(column7), min(column7), count(column7), minmaxrange(column7), "
          + "distinctcount(column7), distinctcounthll(column6), distinctcountrawhll(column7), "
          + "distinctcountsmarthll(column6) from testTable";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column7)", "min(column7)", "count(*)",
      "minmaxrange(column7)", "distinctcount(column7)", "distinctcounthll(column6)", "distinctcountrawhll(column7)",
      "distinctcountsmarthll(column6)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
              DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT,
              DataSchema.ColumnDataType.LONG, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 8);
        assertEquals(resultRow[0], 2.137685743E9);
        assertEquals(resultRow[1], 675695.0);
        assertEquals(resultRow[2], 120000L);
        assertEquals(resultRow[3], 2.137010048E9);
        assertEquals(resultRow[4], 146);
        assertEquals(resultRow[5], 695L);
        assertEquals(resultRow[7], 608);
      }
    }
    {
      // Not allowed aggregation functions on forwardIndexDisabled columns
      String query = "SELECT sum(column7), avg(column6) from testTable";
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
      String query = "SELECT column1, max(column6) from testTable GROUP BY column1";
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
      String query = "SELECT column1, max(column6) from testTable GROUP BY column1 ORDER BY column1";
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
      String query = "SELECT column1, max(column9) from testTable GROUP BY column1 ORDER BY min(column6)";
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
      String query = "SELECT max(column7), min(column6) from testTable WHERE column7 = 675695";
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
      String query = "SELECT max(column1), min(column6) from testTable WHERE column1 > 675695";
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
      String query = "SELECT max(column1), sum(column9) from testTable WHERE column7 = 675695";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 92L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 184L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(column1)", "sum(column9)"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertEquals(resultRow[0], 2.106334109E9);
        assertEquals(resultRow[1], 5.7037961104E10);
      }
    }
    {
      // Allowed aggregation functions on non-forwardIndexDisabled columns with a filter on a forwardIndexDisabled
      // column and group by order by on non-forwardIndexDisabled column
      String query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 675695 GROUP BY "
          + "column1 ORDER BY column1";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 92L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 184L);
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
      String query = "SELECT column1, max(column1), sum(column9) from testTable WHERE column7 = 675695 GROUP BY "
          + "column1 ORDER BY max(column6)";
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
      String query = "SELECT MAX(ADD(column6, column9)) from testTable LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation not involving any forwardIndexDisabled columns
      String query = "SELECT MAX(ADD(column1, column9)) from testTable LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(add(column1,column9))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 1);
        assertEquals(resultRow[0], 4.264013718E9);
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column with group by
      String query = "SELECT column1, MAX(ADD(column6, column9)) from testTable GROUP BY column1 LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation not involving any forwardIndexDisabled column with group by
      String query = "SELECT column1, MAX(ADD(column1, column9)) from testTable GROUP BY column1 LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "max(add(column1,column9))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
      }
    }
    {
      // Transform inside aggregation involving a forwardIndexDisabled column with group by order by
      String query = "SELECT column1, MAX(ADD(column6, column9)) from testTable GROUP BY column1 ORDER BY column1 "
          + "DESC LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in unsupported aggregation query");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // Transform inside aggregation not involving any forwardIndexDisabled column with group by order by
      String query = "SELECT column1, MAX(ADD(column1, column9)) from testTable GROUP BY column1 ORDER BY column1 "
          + "DESC LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"column1", "max(add(column1,column9))"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE}));
      List<Object[]> resultRows = resultTable.getRows();
      int previousVal = Integer.MAX_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertTrue((int) resultRow[0] <= previousVal);
        previousVal = (int) resultRow[0];
      }
    }
  }

  @Test
  public void testSelectWithAggregationGroupByHaving() {
    {
      // forwardIndexDisabled column used in HAVING clause
      String query = "SELECT min(column7), max(column6) from testTable GROUP BY column1 HAVING min(column7) > 675695 "
          + "ORDER BY column1 LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in HAVING clause");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // forwardIndexDisabled column not used in HAVING clause but used in aggregation select
      String query = "SELECT max(column6), min(column9) from testTable GROUP BY column1 HAVING min(column9) > 11270 "
          + "ORDER BY column1 LIMIT 10";
      try {
        getBrokerResponse(query);
        Assert.fail("Query should fail since forwardIndexDisabled on a column in HAVING clause");
      } catch (IllegalStateException e) {
        assertTrue(e.getMessage().contains("Forward index disabled for column:")
            && e.getMessage().contains("cannot create DataFetcher!"));
      }
    }
    {
      // forwardIndexDisabled column not used in HAVING clause or aggregation select
      String query = "SELECT min(column9), column1 from testTable GROUP BY column1, column9 HAVING min(column9) "
          + "> 11270 ORDER BY column9 DESC LIMIT 10";
      BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
      assertTrue(brokerResponseNative.getProcessingExceptions() == null
          || brokerResponseNative.getProcessingExceptions().size() == 0);
      ResultTable resultTable = brokerResponseNative.getResultTable();
      assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
      assertEquals(brokerResponseNative.getTotalDocs(), 120_000L);
      assertEquals(brokerResponseNative.getNumDocsScanned(), 120_000L);
      assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
      assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
      assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 240000L);
      assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
      assertNotNull(brokerResponseNative.getProcessingExceptions());
      assertEquals(brokerResponseNative.getProcessingExceptions().size(), 0);
      assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"min(column9)", "column1"},
          new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.INT}));
      List<Object[]> resultRows = resultTable.getRows();
      double previousVal = Double.MAX_VALUE;
      for (Object[] resultRow : resultRows) {
        assertEquals(resultRow.length, 2);
        assertTrue((double) resultRow[0] <= previousVal);
        previousVal = (double) resultRow[0];
      }
    }
  }
}
