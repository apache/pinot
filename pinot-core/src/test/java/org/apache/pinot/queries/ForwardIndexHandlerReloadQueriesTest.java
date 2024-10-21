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
import java.util.List;
import java.util.Map;
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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * The <code>ForwardIndexHandlerReloadQueriesTest</code> class sets up the index segment for the
 * no forward index multi-value queries test with reload.
 * <p>There are totally 14 columns, 100000 records inside the original Avro file where 10 columns are selected to build
 * the index segment. Selected columns information are as following:
 * <ul>
 *   ColumnName, FieldType, DataType, Cardinality, IsSorted, HasInvertedIndex, IsMultiValue, HasDictionary, RangeIndex
 *   <li>column1, METRIC, INT, 51594, F, F, F, F, F</li>
 *   <li>column2, METRIC, INT, 42242, F, F, F, F, F</li>
 *   <li>column3, DIMENSION, STRING, 5, F, F, F, F, F</li>
 *   <li>column5, DIMENSION, STRING, 9, F, F, F, F, F</li>
 *   <li>column6, DIMENSION, INT, 18499, F, F, T, T, F</li>
 *   <li>column7, DIMENSION, INT, 359, F, F, T, F, F</li>
 *   <li>column8, DIMENSION, INT, 850, F, T, F, T, F</li>
 *   <li>column9, METRIC, INT, 146, F, T, F, T, T</li>
 *   <li>column10, METRIC, INT, 3960, F, F, F, F, T</li>
 *   <li>daysSinceEpoch, TIME, INT, 1, T, F, F, T, F</li>
 * </ul>
 */
public class ForwardIndexHandlerReloadQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), ForwardIndexHandlerReloadQueriesTest.class.getSimpleName());
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

  // Hard-coded query filter
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

    List<String> noDictionaryColumns = List.of("column1", "column2", "column3", "column5", "column7", "column10");
    List<String> invertedIndexColumns = List.of("column8", "column9");
    List<FieldConfig> fieldConfigs = new ArrayList<>(noDictionaryColumns.size());
    for (String column : noDictionaryColumns) {
      fieldConfigs.add(
          new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), FieldConfig.CompressionCodec.SNAPPY, null));
    }
    TableConfig tableConfig = createTableConfig(noDictionaryColumns, invertedIndexColumns, List.of(), fieldConfigs);

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

    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, SCHEMA));
    Map<String, ColumnMetadata> columnMetadataMap = segment.getSegmentMetadata().getColumnMetadataMap();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      String column = entry.getKey();
      ColumnMetadata metadata = entry.getValue();
      assertNotNull(segment.getForwardIndex(column));
      if (noDictionaryColumns.contains(column)) {
        assertFalse(metadata.hasDictionary());
        assertNull(segment.getDictionary(column));
      } else {
        assertTrue(metadata.hasDictionary());
        assertNotNull(segment.getDictionary(column));
      }
      if (invertedIndexColumns.contains(column)) {
        assertNotNull(segment.getInvertedIndex(column));
      }
    }

    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
  }

  private TableConfig createTableConfig(List<String> noDictionaryColumns, List<String> invertedIndexColumns,
      List<String> rangeIndexColumns, List<FieldConfig> fieldConfigs) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME).setTimeColumnName("daysSinceEpoch")
        .setNoDictionaryColumns(noDictionaryColumns).setInvertedIndexColumns(invertedIndexColumns)
        .setRangeIndexColumns(rangeIndexColumns).setFieldConfigList(fieldConfigs).build();
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
  public void testSelectQueries()
      throws Exception {
    String query =
        "SELECT column1, column2, column3, column6, column7, column10 FROM testTable WHERE column10 > 674022574 AND "
            + "column1 > 100000000 AND column2 BETWEEN 20000000 AND 1000000000 AND column3 <> 'w' AND (column6 < "
            + "500000 OR column7 NOT IN (225, 407)) AND daysSinceEpoch = 1756015683 ORDER BY column1";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 1184L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1384L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 913464L);
    assertNotNull(brokerResponseNative.getExceptions());

    DataSchema dataSchema = new DataSchema(new String[]{
        "column1", "column2", "column3", "column6", "column7", "column10"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
        DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT
    });
    assertEquals(resultTable.getDataSchema(), dataSchema);
    List<Object[]> resultRows1 = resultTable.getRows();

    changePropertiesAndReloadSegment();

    // Run the same query again.
    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 1184L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1384L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 250896L);
    assertNotNull(brokerResponseNative.getExceptions());

    dataSchema = new DataSchema(new String[]{
        "column1", "column2", "column3", "column6", "column7", "column10"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
        DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT_ARRAY, DataSchema.ColumnDataType.INT
    });
    assertEquals(resultTable.getDataSchema(), dataSchema);
    List<Object[]> resultRows2 = resultTable.getRows();

    validateBeforeAfterQueryResults(resultRows1, resultRows2);
  }

  @Test
  public void testSelectWithDistinctQueries()
      throws Exception {
    String query =
        "SELECT DISTINCT column1, column2, column3, column6, column7, column9, column10 FROM testTable ORDER BY "
            + "column1 LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 2800000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    DataSchema dataSchema = new DataSchema(new String[]{
        "column1", "column2", "column3", "column6", "column7", "column9", "column10"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.INT
    });
    assertEquals(resultTable.getDataSchema(), dataSchema);
    List<Object[]> resultRows1 = resultTable.getRows();

    changePropertiesAndReloadSegment();

    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 2800000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    dataSchema = new DataSchema(new String[]{
        "column1", "column2", "column3", "column6", "column7", "column9", "column10"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING,
        DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT,
        DataSchema.ColumnDataType.INT
    });
    assertEquals(resultTable.getDataSchema(), dataSchema);
    List<Object[]> resultRows2 = resultTable.getRows();

    validateBeforeAfterQueryResults(resultRows1, resultRows2);
  }

  @Test
  public void testSelectWithGroupByOrderByQueries()
      throws Exception {
    String query =
        "SELECT column1, column7, column9 FROM testTable GROUP BY column1, column7, column9 ORDER BY column1, "
            + "column7, column9 LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1200000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"column1", "column7", "column9"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
        }));
    List<Object[]> resultRows1 = resultTable.getRows();
    int previousVal = -1;
    for (Object[] resultRow : resultRows1) {
      assertEquals(resultRow.length, 3);
      assertTrue((int) resultRow[0] >= previousVal);
      previousVal = (int) resultRow[0];
    }

    changePropertiesAndReloadSegment();

    brokerResponseNative = getBrokerResponse(query);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 1200000L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"column1", "column7", "column9"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT
        }));
    List<Object[]> resultRows2 = resultTable.getRows();
    previousVal = -1;
    for (Object[] resultRow : resultRows2) {
      assertEquals(resultRow.length, 3);
      assertTrue((int) resultRow[0] >= previousVal);
      previousVal = (int) resultRow[0];
    }

    validateBeforeAfterQueryResults(resultRows1, resultRows2);
  }

  @Test
  public void testAllSelectAggregations()
      throws Exception {
    String query =
        "SELECT MAX(column1), MIN(column1), MAX(column2), MIN(column2), MAXMV(column6), MINMV(column6), MAXMV"
            + "(column7), MINMV(column7), MAX(column9), MIN(column9), MAX(column10), MIN(column10) FROM testTable";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{
        "max(column1)", "min(column1)",
        "max" + "(column2)", "min(column2)", "maxmv(column6)", "minmv(column6)",
        "maxmv" + "(column7)", "minmv(column7)", "max(column9)", "min(column9)", "max(column10)", "min(column10)"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
    }));
    List<Object[]> beforeResultRows = resultTable.getRows();

    changePropertiesAndReloadSegment();

    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{
        "max(column1)", "min(column1)",
        "max" + "(column2)", "min(column2)", "maxmv(column6)", "minmv(column6)",
        "maxmv" + "(column7)", "minmv(column7)", "max(column9)", "min(column9)", "max(column10)", "min(column10)"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE,
        DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
    }));
    List<Object[]> afterResultRows = resultTable.getRows();

    validateBeforeAfterQueryResults(beforeResultRows, afterResultRows);
  }

  @Test
  public void testMaxArrayLengthAggregation()
      throws Exception {
    // TEST1 - Before Reload: Test for column7.
    String query1 = "SELECT MAX(ARRAYLENGTH(column7)) from testTable LIMIT 10";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query1);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400000);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(arraylength(column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> beforeResultRows1 = resultTable.getRows();

    // TEST2 - Before Reload: Test for column6.
    String query2 = "SELECT MAX(ARRAYLENGTH(column6)) from testTable LIMIT 10";
    brokerResponseNative = getBrokerResponse(query2);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400000);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(arraylength(column6))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> beforeResultRows2 = resultTable.getRows();

    changePropertiesAndReloadSegment();

    // TEST1 - After Reload: Test for column7.
    brokerResponseNative = getBrokerResponse(query1);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400000);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(arraylength(column7))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> afterResultRows1 = resultTable.getRows();
    validateBeforeAfterQueryResults(beforeResultRows1, afterResultRows1);

    // TEST2 - After Reload: Test for column6.
    brokerResponseNative = getBrokerResponse(query2);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 400_000L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 400000);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(), new DataSchema(new String[]{"max(arraylength(column6))"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE}));
    List<Object[]> afterResultRows2 = resultTable.getRows();
    validateBeforeAfterQueryResults(beforeResultRows2, afterResultRows2);
  }

  @Test
  public void testSelectWithAggregationQueries()
      throws Exception {
    // TEST1 - Before Reload: Test where column7 is in filter.
    String query1 = "SET \"timeoutMs\" = 30000; SELECT column1, max(column1), sum(column10) from testTable WHERE "
        + "column7 = 2147483647 GROUP BY column1 ORDER BY column1";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query1);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    ResultTable resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 536360L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"column1", "max(column1)", "sum(column10)"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
        }));
    List<Object[]> beforeResultRows1 = resultTable.getRows();

    // TEST2 - Before Reload: Test where column6 is in filter.
    String query2 = "SELECT column1, max(column1), sum(column10) from testTable WHERE column6 = 1001 GROUP BY "
        + "column1 ORDER BY column1";
    brokerResponseNative = getBrokerResponse(query2);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 8);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 16L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 426752L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"column1", "max(column1)", "sum(column10)"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
        }));
    List<Object[]> beforeResultRows2 = resultTable.getRows();

    changePropertiesAndReloadSegment();

    // TEST1 - After reload. Test where column7 is in filter.
    brokerResponseNative = getBrokerResponse(query1);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 10);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 199_756L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 399_512L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"column1", "max(column1)", "sum(column10)"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
        }));
    List<Object[]> afterResultRows1 = resultTable.getRows();

    validateBeforeAfterQueryResults(beforeResultRows1, afterResultRows1);

    // TEST2 - After Reload: Test where column6 is in filter.
    brokerResponseNative = getBrokerResponse(query2);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 8);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 16L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 426752);
    assertNotNull(brokerResponseNative.getExceptions());
    assertEquals(brokerResponseNative.getExceptions().size(), 0);
    assertEquals(resultTable.getDataSchema(),
        new DataSchema(new String[]{"column1", "max(column1)", "sum(column10)"}, new DataSchema.ColumnDataType[]{
            DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.DOUBLE, DataSchema.ColumnDataType.DOUBLE
        }));
    List<Object[]> afterResultRows2 = resultTable.getRows();

    validateBeforeAfterQueryResults(beforeResultRows2, afterResultRows2);
  }

  @Test
  public void testRangeIndexAfterReload()
      throws Exception {
    String query = "select count(*) from testTable where column10 > 674022574 and column9 < 674022574";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    ResultTable resultTable1 = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40224L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 479412L);
    assertNotNull(brokerResponseNative.getExceptions());

    DataSchema dataSchema = new DataSchema(new String[]{
        "count(*)"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.LONG
    });
    assertEquals(resultTable1.getDataSchema(), dataSchema);
    List<Object[]> resultRows1 = resultTable1.getRows();

    changePropertiesAndReloadSegment();

    brokerResponseNative = getBrokerResponse(query);
    assertTrue(brokerResponseNative.getExceptions() == null || brokerResponseNative.getExceptions().size() == 0);
    resultTable1 = brokerResponseNative.getResultTable();
    assertEquals(brokerResponseNative.getNumRowsResultSet(), 1);
    assertEquals(brokerResponseNative.getTotalDocs(), 400_000L);
    assertEquals(brokerResponseNative.getNumDocsScanned(), 40224L);
    assertEquals(brokerResponseNative.getNumSegmentsProcessed(), 4L);
    assertEquals(brokerResponseNative.getNumSegmentsMatched(), 4L);
    assertEquals(brokerResponseNative.getNumEntriesScannedPostFilter(), 0L);
    assertEquals(brokerResponseNative.getNumEntriesScannedInFilter(), 0L);
    assertNotNull(brokerResponseNative.getExceptions());

    dataSchema = new DataSchema(new String[]{
        "count(*)"
    }, new DataSchema.ColumnDataType[]{
        DataSchema.ColumnDataType.LONG
    });
    assertEquals(resultTable1.getDataSchema(), dataSchema);
    List<Object[]> resultRows2 = resultTable1.getRows();

    validateBeforeAfterQueryResults(resultRows1, resultRows2);
  }

  private void validateBeforeAfterQueryResults(List<Object[]> beforeResults, List<Object[]> afterResults) {
    assertEquals(beforeResults.size(), afterResults.size());
    for (int i = 0; i < beforeResults.size(); i++) {
      Object[] resultRow1 = beforeResults.get(i);
      Object[] resultRow2 = afterResults.get(i);
      assertEquals(resultRow1.length, resultRow2.length);
      for (int j = 0; j < resultRow1.length; j++) {
        assertEquals(resultRow1[j], resultRow2[j]);
      }
    }
  }

  /**
   * As a part of segmentReload, the ForwardIndexHandler will perform the following operations:
   *
   * column1 -> change compression.
   * column6 -> disable dictionary
   * column9 -> disable dictionary
   * column3 -> Enable dictionary.
   * column2 -> Enable dictionary. Add inverted index.
   * column7 -> Enable dictionary. Add inverted index.
   * column10 -> Enable dictionary.
   */
  private void changePropertiesAndReloadSegment()
      throws Exception {
    List<String> noDictionaryColumns = List.of("column1", "column5", "column6", "column9");
    List<String> invertedIndexColumns = List.of("column2", "column7", "column8");
    List<String> rangeIndexColumns = List.of("column9", "column10");
    List<FieldConfig> fieldConfigs = new ArrayList<>(noDictionaryColumns.size());
    for (String column : noDictionaryColumns) {
      FieldConfig.CompressionCodec compressionCodec = FieldConfig.CompressionCodec.SNAPPY;
      if (column.equals("column1")) {
        compressionCodec = FieldConfig.CompressionCodec.ZSTANDARD;
      }
      fieldConfigs.add(new FieldConfig(column, FieldConfig.EncodingType.RAW, List.of(), compressionCodec, null));
    }
    TableConfig tableConfig =
        createTableConfig(noDictionaryColumns, invertedIndexColumns, rangeIndexColumns, fieldConfigs);
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig(tableConfig, SCHEMA);

    // Reload the segments to pick up the new configs
    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    ImmutableSegment segment = reloadSegment(indexDir, indexLoadingConfig, SCHEMA);
    _indexSegment.destroy();
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);

    Map<String, ColumnMetadata> columnMetadataMap = segment.getSegmentMetadata().getColumnMetadataMap();
    for (Map.Entry<String, ColumnMetadata> entry : columnMetadataMap.entrySet()) {
      String column = entry.getKey();
      ColumnMetadata metadata = entry.getValue();
      assertNotNull(segment.getForwardIndex(column));
      if (noDictionaryColumns.contains(column)) {
        assertFalse(metadata.hasDictionary());
        assertNull(segment.getDictionary(column));
      } else {
        assertTrue(metadata.hasDictionary());
        assertNotNull(segment.getDictionary(column));
      }
      if (invertedIndexColumns.contains(column)) {
        assertNotNull(segment.getInvertedIndex(column));
      }
    }
  }
}
