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
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.SegmentPreProcessor;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;
import static org.testng.Assert.assertEquals;


public class CustomReloadQueriesTest extends BaseQueriesTest {

  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), CustomReloadQueriesTest.class.getSimpleName());
  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;

  @BeforeMethod
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private TableConfig createTableConfig(List<String> noDictionaryColumns, List<String> invertedIndexColumns,
      List<String> rangeIndexColumns, List<FieldConfig> fieldConfigs) {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
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

  @DataProvider(name = "alphabets")
  public static Object[][] alphabets() {
    // 2 sets of input data - sorted and unsorted
    return new Object[][] {
        { new String[]{"a", "b", "c", "d", "e"} },
        { new String[]{"b", "c", "a", "e", "d"} }
    };
  }

  /**
   * If a columns approximate cardinality is lesser than actual cardinality and its bits per element also reduces
   * because of this, then enabling dictionary for that column should result in updating both bits per element
   * and cardinality. In this test, we will verify both segment metadata and query results
   * @throws Exception
   */
  @Test(dataProvider = "alphabets")
  public void testReducedBitsPerElementWithNoDictCardinalityApproximation(String[] alphabets)
      throws Exception {

    // Common variables - schema, data file, etc
    File csvFile = new File(FileUtils.getTempDirectory(), "data.csv");
    List<String> values = new ArrayList<>(Arrays.asList(alphabets));
    String columnName = "column1";
    writeCsv(csvFile, values, columnName);
    Schema schema = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
        .addSingleValueDimension(columnName, FieldSpec.DataType.STRING)
        .build();

    // Load segment with no dictionary column and get segment metadata
    List<FieldConfig> fieldConfigs = new ArrayList<>();
    fieldConfigs.add(new FieldConfig(
        columnName, FieldConfig.EncodingType.RAW, List.of(), FieldConfig.CompressionCodec.SNAPPY, null));
    TableConfig tableConfig = createTableConfig(List.of(), List.of(), List.of(), fieldConfigs);
    ImmutableSegment segment = buildNewSegment(tableConfig, schema, csvFile.getAbsolutePath());
    Map<String, ColumnMetadata> columnMetadataMap = segment.getSegmentMetadata().getColumnMetadataMap();

    ColumnMetadata columnMetadata1 = columnMetadataMap.get(columnName);
    assertFalse(columnMetadata1.hasDictionary());
    assertNull(segment.getDictionary(columnName));

    String query = "SELECT column1, count(*) FROM testTable GROUP BY column1 ORDER BY column1";
    BrokerResponseNative brokerResponseNative = getBrokerResponse(query);
    List<Object[]> resultRows1 = brokerResponseNative.getResultTable().getRows();
    assertEquals(resultRows1.size(), 5);

    // Make column1 dictionary encoded and reload table
    fieldConfigs = new ArrayList<>();
    fieldConfigs.add(new FieldConfig(
        columnName, FieldConfig.EncodingType.DICTIONARY, List.of(), FieldConfig.CompressionCodec.SNAPPY, null));
    tableConfig = createTableConfig(List.of(), List.of(), List.of(), fieldConfigs);
    segment = reloadSegment(tableConfig, schema);
    columnMetadataMap = segment.getSegmentMetadata().getColumnMetadataMap();

    ColumnMetadata columnMetadata2 = columnMetadataMap.get(columnName);
    assertEquals(columnMetadata2.getCardinality(), 5); // actual cardinality
    assertEquals(columnMetadata2.getBitsPerElement(), 3); // actual required bits per element
    assertTrue(columnMetadata2.hasDictionary());
    assertNotNull(segment.getDictionary(columnName));

    brokerResponseNative = getBrokerResponse(query);
    List<Object[]> resultRows2 = brokerResponseNative.getResultTable().getRows();
    validateBeforeAfterQueryResults(resultRows1, resultRows2);
  }

  private ImmutableSegment buildNewSegment(TableConfig tableConfig, Schema schema, String inputFile)
      throws Exception {
    SegmentGeneratorConfig generatorConfig = new SegmentGeneratorConfig(tableConfig, schema);
    generatorConfig.setInputFilePath(inputFile);
    generatorConfig.setFormat(FileFormat.CSV);
    generatorConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    generatorConfig.setSegmentName(SEGMENT_NAME);

    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(generatorConfig);
    driver.build();

    ImmutableSegment segment = ImmutableSegmentLoader.load(
        new File(INDEX_DIR, SEGMENT_NAME), new IndexLoadingConfig(tableConfig, schema));
    if (_indexSegment != null) {
      _indexSegment.destroy();
    }
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
    return segment;
  }

  private ImmutableSegment reloadSegment(TableConfig tableConfig, Schema schema)
      throws Exception {
    IndexLoadingConfig loadingConfig = new IndexLoadingConfig(tableConfig, schema);
    File indexDir = new File(INDEX_DIR, SEGMENT_NAME);
    SegmentDirectory segmentDirectory = new SegmentLocalFSDirectory(indexDir, ReadMode.mmap);
    try (SegmentPreProcessor preProcessor = new SegmentPreProcessor(segmentDirectory, loadingConfig)) {
      preProcessor.process();
    }
    // Replace in-memory segment with reloaded one
    _indexSegment.destroy();
    ImmutableSegment segment = ImmutableSegmentLoader.load(indexDir, loadingConfig);
    _indexSegment = segment;
    _indexSegments = List.of(segment, segment);
    return segment;
  }

  private static void writeCsv(File file, List<String> values, String columnName) throws IOException {
    try (FileWriter writer = new FileWriter(file, false)) {
      writer.append(columnName).append('\n');
      for (String v : values) {
        writer.append(v).append('\n');
      }
    }
  }
}
