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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.pinot.core.operator.blocks.results.SelectionResultsBlock;
import org.apache.pinot.core.operator.query.SelectionOnlyOperator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Functional tests for compression type feature.
 * The tests use three kinds of input data
 * (1) string
 * (2) integer
 * (3) long
 */
public class NoDictionaryCompressionQueriesTest extends BaseQueriesTest {
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "CompressionCodecQueriesTest");
  private static final String TABLE_NAME = "MyTable";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String SNAPPY_STRING = "SNAPPY_STRING";
  private static final String PASS_THROUGH_STRING = "PASS_THROUGH_STRING";
  private static final String ZSTANDARD_STRING = "ZSTANDARD_STRING";
  private static final String LZ4_STRING = "LZ4_STRING";

  private static final String SNAPPY_LONG = "SNAPPY_LONG";
  private static final String PASS_THROUGH_LONG = "PASS_THROUGH_LONG";
  private static final String ZSTANDARD_LONG = "ZSTANDARD_LONG";
  private static final String LZ4_LONG = "LZ4_LONG";

  private static final String SNAPPY_INTEGER = "SNAPPY_INTEGER";
  private static final String PASS_THROUGH_INTEGER = "PASS_THROUGH_INTEGER";
  private static final String ZSTANDARD_INTEGER = "ZSTANDARD_INTEGER";
  private static final String LZ4_INTEGER = "LZ4_INTEGER";

  private static final List<String> RAW_SNAPPY_INDEX_COLUMNS =
      Arrays.asList(SNAPPY_STRING, SNAPPY_LONG, SNAPPY_INTEGER);

  private static final List<String> RAW_ZSTANDARD_INDEX_COLUMNS =
      Arrays.asList(ZSTANDARD_STRING, ZSTANDARD_LONG, ZSTANDARD_INTEGER);

  private static final List<String> RAW_PASS_THROUGH_INDEX_COLUMNS =
      Arrays.asList(PASS_THROUGH_STRING, PASS_THROUGH_LONG, PASS_THROUGH_INTEGER);

  private static final List<String> RAW_LZ4_INDEX_COLUMNS = Arrays.asList(LZ4_STRING, LZ4_LONG, LZ4_INTEGER);

  private IndexSegment _indexSegment;
  private List<IndexSegment> _indexSegments;
  private List<GenericRow> _rows;

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

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);

    buildSegment();

    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    Set<String> indexColumns = new HashSet<>();
    indexColumns.addAll(RAW_SNAPPY_INDEX_COLUMNS);
    indexColumns.addAll(RAW_PASS_THROUGH_INDEX_COLUMNS);
    indexColumns.addAll(RAW_ZSTANDARD_INDEX_COLUMNS);
    indexColumns.addAll(RAW_LZ4_INDEX_COLUMNS);

    indexLoadingConfig.addNoDictionaryColumns(indexColumns);
    ImmutableSegment immutableSegment =
        ImmutableSegmentLoader.load(new File(INDEX_DIR, SEGMENT_NAME), indexLoadingConfig);
    _indexSegment = immutableSegment;
    _indexSegments = Arrays.asList(immutableSegment, immutableSegment);
  }

  @AfterClass
  public void tearDown() {
    _indexSegment.destroy();
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  private void buildSegment()
      throws Exception {
    _rows = createTestData();

    List<FieldConfig> fieldConfigs = new ArrayList<>(
        RAW_SNAPPY_INDEX_COLUMNS.size() + RAW_ZSTANDARD_INDEX_COLUMNS.size() + RAW_PASS_THROUGH_INDEX_COLUMNS.size()
            + RAW_LZ4_INDEX_COLUMNS.size());

    for (String indexColumn : RAW_SNAPPY_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.SNAPPY, null));
    }

    for (String indexColumn : RAW_ZSTANDARD_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.ZSTANDARD, null));
    }

    for (String indexColumn : RAW_PASS_THROUGH_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.PASS_THROUGH, null));
    }

    for (String indexColumn : RAW_LZ4_INDEX_COLUMNS) {
      fieldConfigs.add(new FieldConfig(indexColumn, FieldConfig.EncodingType.RAW, Collections.emptyList(),
          FieldConfig.CompressionCodec.LZ4, null));
    }

    List<String> noDictionaryColumns = new ArrayList<>();
    noDictionaryColumns.addAll(RAW_SNAPPY_INDEX_COLUMNS);
    noDictionaryColumns.addAll(RAW_ZSTANDARD_INDEX_COLUMNS);
    noDictionaryColumns.addAll(RAW_PASS_THROUGH_INDEX_COLUMNS);
    noDictionaryColumns.addAll(RAW_LZ4_INDEX_COLUMNS);

    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).setNoDictionaryColumns(noDictionaryColumns)
            .setFieldConfigList(fieldConfigs).build();
    Schema schema = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
        .addSingleValueDimension(SNAPPY_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(PASS_THROUGH_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(ZSTANDARD_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(LZ4_STRING, FieldSpec.DataType.STRING)
        .addSingleValueDimension(SNAPPY_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(ZSTANDARD_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(PASS_THROUGH_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(LZ4_INTEGER, FieldSpec.DataType.INT)
        .addSingleValueDimension(SNAPPY_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(ZSTANDARD_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(PASS_THROUGH_LONG, FieldSpec.DataType.LONG)
        .addSingleValueDimension(LZ4_LONG, FieldSpec.DataType.LONG).build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(INDEX_DIR.getPath());
    config.setTableName(TABLE_NAME);
    config.setSegmentName(SEGMENT_NAME);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    try (RecordReader recordReader = new GenericRowRecordReader(_rows)) {
      driver.init(config, recordReader);
      driver.build();
    }
  }

  private List<GenericRow> createTestData() {
    List<GenericRow> rows = new ArrayList<>();

    //Generate random data
    int rowLength = 1000;
    Random random = new Random();
    String[] tempStringRows = new String[rowLength];
    Integer[] tempIntRows = new Integer[rowLength];
    Long[] tempLongRows = new Long[rowLength];

    for (int i = 0; i < rowLength; i++) {
      //Adding a fixed value to check for filter queries
      if (i % 10 == 0) {
        tempStringRows[i] = "hello_world_123";
        tempIntRows[i] = 1001;
        tempLongRows[i] = 1001L;
      } else {
        tempStringRows[i] = RandomStringUtils.random(random.nextInt(100), true, true);
        tempIntRows[i] = RandomUtils.nextInt(0, rowLength);
        tempLongRows[i] = RandomUtils.nextLong(0, rowLength);
      }
    }

    for (int i = 0; i < rowLength; i++) {
      GenericRow row = new GenericRow();
      row.putValue(SNAPPY_STRING, tempStringRows[i]);
      row.putValue(ZSTANDARD_STRING, tempStringRows[i]);
      row.putValue(PASS_THROUGH_STRING, tempStringRows[i]);
      row.putValue(LZ4_STRING, tempStringRows[i]);
      row.putValue(SNAPPY_INTEGER, tempIntRows[i]);
      row.putValue(ZSTANDARD_INTEGER, tempIntRows[i]);
      row.putValue(PASS_THROUGH_INTEGER, tempIntRows[i]);
      row.putValue(LZ4_INTEGER, tempIntRows[i]);
      row.putValue(SNAPPY_LONG, tempLongRows[i]);
      row.putValue(ZSTANDARD_LONG, tempLongRows[i]);
      row.putValue(PASS_THROUGH_LONG, tempLongRows[i]);
      row.putValue(LZ4_LONG, tempLongRows[i]);
      rows.add(row);
    }
    return rows;
  }

  /**
   * Tests for basic compression codec queries.
   */
  @Test
  public void testQueriesWithCompressionCodec() {
    String query = "SELECT SNAPPY_STRING, ZSTANDARD_STRING, PASS_THROUGH_STRING, LZ4_STRING, "
        + "SNAPPY_INTEGER, ZSTANDARD_INTEGER, PASS_THROUGH_INTEGER, LZ4_INTEGER, "
        + "SNAPPY_LONG, ZSTANDARD_LONG, PASS_THROUGH_LONG, LZ4_LONG FROM MyTable LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      expected.add(new Serializable[]{
          String.valueOf(row.getValue(SNAPPY_STRING)), String.valueOf(row.getValue(ZSTANDARD_STRING)),
          String.valueOf(row.getValue(PASS_THROUGH_STRING)), String.valueOf(row.getValue(LZ4_STRING)),
          (Integer) row.getValue(SNAPPY_INTEGER), (Integer) row.getValue(ZSTANDARD_INTEGER),
          (Integer) row.getValue(PASS_THROUGH_INTEGER), (Integer) row.getValue(LZ4_INTEGER),
          (Long) row.getValue(SNAPPY_LONG), (Long) row.getValue(ZSTANDARD_LONG), (Long) row.getValue(PASS_THROUGH_LONG),
          (Long) row.getValue(LZ4_LONG)
      });
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over integer values compression codec queries.
   */
  @Test
  public void testZstandardIntegerFilterQueriesWithCompressionCodec() {
    String query = "SELECT ZSTANDARD_INTEGER FROM MyTable WHERE ZSTANDARD_INTEGER > 1000 LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      int value = (Integer) row.getValue(ZSTANDARD_INTEGER);
      if (value > 1000) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over integer values LZ4 compression codec queries.
   */
  @Test
  public void testLZ4IntegerFilterQueriesWithCompressionCodec() {
    String query = "SELECT LZ4_INTEGER FROM MyTable WHERE LZ4_INTEGER > 1000 LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      int value = (Integer) row.getValue(LZ4_INTEGER);
      if (value > 1000) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over integer values compression codec queries.
   */
  @Test
  public void testSnappyIntegerFilterQueriesWithCompressionCodec() {
    String query = "SELECT SNAPPY_INTEGER FROM MyTable WHERE SNAPPY_INTEGER > 100 LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      int value = (Integer) row.getValue(SNAPPY_INTEGER);
      if (value > 100) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over integer values compression codec queries.
   */
  @Test
  public void testPassThroughIntegerFilterQueriesWithCompressionCodec() {
    String query = "SELECT PASS_THROUGH_INTEGER FROM MyTable WHERE PASS_THROUGH_INTEGER > 100 LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      int value = (Integer) row.getValue(PASS_THROUGH_INTEGER);
      if (value > 100) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over string values zstandard compression codec queries.
   */
  @Test
  public void testZstandardStringFilterQueriesWithCompressionCodec() {
    String query = "SELECT ZSTANDARD_STRING FROM MyTable WHERE ZSTANDARD_STRING = 'hello_world_123' LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      String value = String.valueOf(row.getValue(ZSTANDARD_STRING));
      if (value.equals("hello_world_123")) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over string values LZ4 compression codec queries.
   */
  @Test
  public void testLZ4StringFilterQueriesWithCompressionCodec() {
    String query = "SELECT LZ4_STRING FROM MyTable WHERE LZ4_STRING = 'hello_world_123' LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      String value = String.valueOf(row.getValue(LZ4_STRING));
      if (value.equals("hello_world_123")) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /**
   * Tests for filter over string values snappy compression codec queries.
   */
  @Test
  public void testSnappyStringFilterQueriesWithCompressionCodec() {
    String query = "SELECT SNAPPY_STRING FROM MyTable WHERE SNAPPY_STRING = 'hello_world_123' LIMIT 1000";
    ArrayList<Serializable[]> expected = new ArrayList<>();

    for (GenericRow row : _rows) {
      String value = String.valueOf(row.getValue(SNAPPY_STRING));
      if (value.equals("hello_world_123")) {
        expected.add(new Serializable[]{value});
      }
    }
    testSelectQueryHelper(query, expected.size(), expected);
  }

  /*
   * Helper methods for tests
   */
  private void testSelectQueryHelper(String query, int expectedResultSize, List<Serializable[]> expectedResults) {
    SelectionOnlyOperator operator = getOperator(query);
    SelectionResultsBlock operatorResult = operator.nextBlock();
    List<Object[]> resultset = (List<Object[]>) operatorResult.getRows();
    Assert.assertNotNull(resultset);
    Assert.assertEquals(resultset.size(), expectedResultSize);
    if (expectedResults != null) {
      // compare with expected result table
      for (int i = 0; i < expectedResultSize; i++) {
        Object[] actualRow = resultset.get(i);
        Object[] expectedRow = expectedResults.get(i);
        Assert.assertEquals(actualRow.length, expectedRow.length);
        for (int j = 0; j < actualRow.length; j++) {
          Object actualColValue = actualRow[j];
          Object expectedColValue = expectedRow[j];
          Assert.assertEquals(actualColValue, expectedColValue);
        }
      }
    }
  }
}
