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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReaderFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.ColumnReader;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Tests for columnar segment building functionality.
 *
 * <p>This test class validates the new columnar segment building capability including:
 * <ul>
 *   <li>ColumnReader interface implementations</li>
 *   <li>ColumnReaderFactory for different data sources</li>
 *   <li>Columnar segment building vs row-major building equivalence</li>
 *   <li>Support for new columns with default values</li>
 * </ul>
 */
public class ColumnarSegmentBuildingTest {
  private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";

  // Test columns
  private static final String STRING_COL_1 = "stringCol1";
  private static final String STRING_COL_2 = "stringCol2";
  private static final String INT_COL_1 = "intCol1";
  private static final String INT_COL_2 = "intCol2";
  private static final String LONG_COL = "longCol";
  private static final String FLOAT_COL = "floatCol";
  private static final String DOUBLE_COL = "doubleCol";
  private static final String BIG_DECIMAL_COL = "bigDecimalCol";
  private static final String BYTES_COL = "bytesCol";
  private static final String TIME_COL = "timeCol";
  private static final String MV_INT_COL = "mvIntCol";
  private static final String MV_STRING_COL = "mvStringCol";

  // New column for testing default value handling
  private static final String NEW_STRING_COL = "newStringCol";
  private static final String NEW_INT_COL = "newIntCol";

  private File _tempDir;
  private Schema _originalSchema;
  private Schema _extendedSchema; // Schema with additional columns
  private TableConfig _tableConfig;
  private List<GenericRow> _testData;

  @BeforeClass
  public void setUp() throws IOException {
    _tempDir = new File(TEMP_DIR, "ColumnarSegmentBuildingTest");
    FileUtils.deleteQuietly(_tempDir);
    _tempDir.mkdirs();

    // Create original schema
    _originalSchema = new Schema.SchemaBuilder()
            .addSingleValueDimension(STRING_COL_1, FieldSpec.DataType.STRING)
            .addSingleValueDimension(STRING_COL_2, FieldSpec.DataType.STRING)
            .addSingleValueDimension(INT_COL_1, FieldSpec.DataType.INT)
            .addSingleValueDimension(INT_COL_2, FieldSpec.DataType.INT)
            .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
            .addSingleValueDimension(FLOAT_COL, FieldSpec.DataType.FLOAT)
            .addSingleValueDimension(DOUBLE_COL, FieldSpec.DataType.DOUBLE)
            .addSingleValueDimension(BIG_DECIMAL_COL, FieldSpec.DataType.BIG_DECIMAL)
            .addSingleValueDimension(BYTES_COL, FieldSpec.DataType.BYTES)
            .addMultiValueDimension(MV_INT_COL, FieldSpec.DataType.INT)
            .addMultiValueDimension(MV_STRING_COL, FieldSpec.DataType.STRING)
            .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
            .build();

    // Create extended schema with additional columns
    _extendedSchema = new Schema.SchemaBuilder()
            .addSingleValueDimension(STRING_COL_1, FieldSpec.DataType.STRING)
            .addSingleValueDimension(STRING_COL_2, FieldSpec.DataType.STRING)
            .addSingleValueDimension(INT_COL_1, FieldSpec.DataType.INT)
            .addSingleValueDimension(INT_COL_2, FieldSpec.DataType.INT)
            .addSingleValueDimension(LONG_COL, FieldSpec.DataType.LONG)
            .addSingleValueDimension(FLOAT_COL, FieldSpec.DataType.FLOAT)
            .addSingleValueDimension(DOUBLE_COL, FieldSpec.DataType.DOUBLE)
            .addSingleValueDimension(BIG_DECIMAL_COL, FieldSpec.DataType.BIG_DECIMAL)
            .addSingleValueDimension(BYTES_COL, FieldSpec.DataType.BYTES)
            .addMultiValueDimension(MV_INT_COL, FieldSpec.DataType.INT)
            .addMultiValueDimension(MV_STRING_COL, FieldSpec.DataType.STRING)
            .addDateTime(TIME_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
            .addSingleValueDimension(NEW_STRING_COL, FieldSpec.DataType.STRING)
            .addSingleValueDimension(NEW_INT_COL, FieldSpec.DataType.INT)
            .build();

    // Create table config
    _tableConfig = new TableConfigBuilder(TableType.OFFLINE)
            .setTableName(TABLE_NAME)
            .setTimeColumnName(TIME_COL)
            .setInvertedIndexColumns(Lists.newArrayList(STRING_COL_1, INT_COL_1))
            .setSortedColumn(INT_COL_1)
            .build();

    // Generate test data
    _testData = generateTestData(100);
  }

  @AfterClass
  public void tearDown() throws IOException {
    FileUtils.deleteQuietly(_tempDir);
  }

  @Test
  public void testBasicColumnarBuilding() throws Exception {
    // First create a segment using traditional row-major approach
    File rowMajorSegmentDir = createRowMajorSegment();

    // Then create a segment using columnar approach from the row-major segment
    File columnarSegmentDir = createColumnarSegment(rowMajorSegmentDir);

    // Validate that both segments have identical data
    validateSegmentsIdentical(rowMajorSegmentDir, columnarSegmentDir);
  }

  @Test
  public void testColumnarBuildingWithNewColumns() throws Exception {
    // Create original segment with original schema
    File originalSegmentDir = createRowMajorSegment();

    // Create new segment with extended schema (has additional columns)
    File newSegmentDir = createColumnarSegmentWithNewColumns(originalSegmentDir);

    // Validate that the new segment has the additional columns with default values
    validateSegmentWithNewColumns(newSegmentDir);
  }

  @Test
  public void testColumnReaderFactory() throws Exception {
    // Create a segment to test column reader factory with
    File segmentDir = createRowMajorSegment();
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    try {
      // Test PinotSegmentColumnReaderFactory
      try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(segment)) {
        factory.init(_originalSchema);

        // Test that all expected columns are available
        Set<String> availableColumns = factory.getAvailableColumns();
        Assert.assertTrue(availableColumns.contains(STRING_COL_1));
        Assert.assertTrue(availableColumns.contains(INT_COL_1));
        Assert.assertTrue(availableColumns.contains(MV_INT_COL));

        // Test creating individual column readers
        ColumnReader stringReader = factory.createColumnReader(STRING_COL_1,
            _originalSchema.getFieldSpecFor(STRING_COL_1));
        Assert.assertEquals(stringReader.getColumnName(), STRING_COL_1);

        ColumnReader mvReader = factory.createColumnReader(MV_INT_COL,
            _originalSchema.getFieldSpecFor(MV_INT_COL));
        Assert.assertEquals(mvReader.getColumnName(), MV_INT_COL);

        // Test reading values using iterator pattern
        Assert.assertTrue(stringReader.hasNext());
        Object firstStringValue = stringReader.next();
        Assert.assertEquals(firstStringValue, "string1_0");

        // Test that we can continue reading
        Assert.assertTrue(stringReader.hasNext());
        Object secondStringValue = stringReader.next();
        Assert.assertEquals(secondStringValue, "string1_1");

        // Reset and test again
        stringReader.rewind();
        Assert.assertTrue(stringReader.hasNext());
        firstStringValue = stringReader.next();
        Assert.assertEquals(firstStringValue, "string1_0");

        // Test creating all column readers
        Map<String, ColumnReader> allReaders = factory.getAllColumnReaders();
        Assert.assertEquals(allReaders.size(), _originalSchema.getPhysicalColumnNames().size());
      }
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testColumnReaderWithNewColumns() throws Exception {
    // Create a segment to test with
    File segmentDir = createRowMajorSegment();
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    try {
      // Test creating readers for new columns (should return default value readers)
      try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(segment)) {
        factory.init(_extendedSchema);

        // Test creating reader for new column
        ColumnReader newStringReader = factory.createColumnReader(NEW_STRING_COL,
            _extendedSchema.getFieldSpecFor(NEW_STRING_COL));
        Assert.assertEquals(newStringReader.getColumnName(), NEW_STRING_COL);

        // Verify it returns default values using iterator pattern
        Assert.assertTrue(newStringReader.hasNext());
        Object defaultValue = newStringReader.next();
        Assert.assertEquals(defaultValue, _extendedSchema.getFieldSpecFor(NEW_STRING_COL).getDefaultNullValue());

        // Test that all values are the same (default)
        int valueCount = 1; // We already read one value
        while (newStringReader.hasNext()) {
          Object value = newStringReader.next();
          Assert.assertEquals(value, defaultValue);
          valueCount++;
        }
        Assert.assertEquals(valueCount, _testData.size());
      }
    } finally {
      segment.destroy();
    }
  }

  @Test
  public void testAllDataTypes() throws Exception {
    // This test validates that all supported data types work correctly with columnar building
    File rowMajorSegmentDir = createRowMajorSegment();
    File columnarSegmentDir = createColumnarSegment(rowMajorSegmentDir);

    // Validate that both segments have identical data for all data types
    validateSegmentsIdentical(rowMajorSegmentDir, columnarSegmentDir);

    // Additionally validate that all expected columns and data types are present
    ImmutableSegment segment = ImmutableSegmentLoader.load(columnarSegmentDir, ReadMode.mmap);
    try {
      Set<String> columnNames = segment.getPhysicalColumnNames();

      // Validate all data types are present
      Assert.assertTrue(columnNames.contains(STRING_COL_1), "STRING column missing");
      Assert.assertTrue(columnNames.contains(INT_COL_1), "INT column missing");
      Assert.assertTrue(columnNames.contains(LONG_COL), "LONG column missing");
      Assert.assertTrue(columnNames.contains(FLOAT_COL), "FLOAT column missing");
      Assert.assertTrue(columnNames.contains(DOUBLE_COL), "DOUBLE column missing");
      Assert.assertTrue(columnNames.contains(BIG_DECIMAL_COL), "BIG_DECIMAL column missing");
      Assert.assertTrue(columnNames.contains(BYTES_COL), "BYTES column missing");
      Assert.assertTrue(columnNames.contains(MV_INT_COL), "Multi-value INT column missing");
      Assert.assertTrue(columnNames.contains(MV_STRING_COL), "Multi-value STRING column missing");
      Assert.assertTrue(columnNames.contains(TIME_COL), "TIME column missing");
    } finally {
      segment.destroy();
    }
  }



  private File createRowMajorSegment() throws Exception {
    File outputDir = new File(_tempDir, "rowMajorSegment");
    FileUtils.deleteQuietly(outputDir);
    outputDir.mkdirs();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _originalSchema);
    config.setOutDir(outputDir.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME + "_rowMajor");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    TestRecordReader recordReader = new TestRecordReader(_testData);
    driver.init(config, recordReader);
    driver.build();

    return new File(outputDir, SEGMENT_NAME + "_rowMajor");
  }

  private File createColumnarSegment(File sourceSegmentDir) throws Exception {
    File outputDir = new File(_tempDir, "columnarSegment");
    FileUtils.deleteQuietly(outputDir);
    outputDir.mkdirs();

    // Load the source segment
    ImmutableSegment sourceSegment = ImmutableSegmentLoader.load(sourceSegmentDir, ReadMode.mmap);

    try {
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _originalSchema);
      config.setOutDir(outputDir.getAbsolutePath());
      config.setSegmentName(SEGMENT_NAME + "_columnar");

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

      // Use the new columnar building approach with config
      try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(sourceSegment)) {
        driver.init(config, factory);
        driver.build();
      }

      return new File(outputDir, SEGMENT_NAME + "_columnar");
    } finally {
      sourceSegment.destroy();
    }
  }

  private File createColumnarSegmentWithNewColumns(File sourceSegmentDir) throws Exception {
    File outputDir = new File(_tempDir, "columnarSegmentWithNewColumns");
    FileUtils.deleteQuietly(outputDir);
    outputDir.mkdirs();

    // Load the source segment
    ImmutableSegment sourceSegment = ImmutableSegmentLoader.load(sourceSegmentDir, ReadMode.mmap);

    try {
      // Use extended schema with new columns
      SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _extendedSchema);
      config.setOutDir(outputDir.getAbsolutePath());
      config.setSegmentName(SEGMENT_NAME + "_withNewColumns");

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

      // Use columnar building with extended schema
      try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(sourceSegment)) {
        driver.init(config, factory);
        driver.build();
      }

      return new File(outputDir, SEGMENT_NAME + "_withNewColumns");
    } finally {
      sourceSegment.destroy();
    }
  }

  private void validateSegmentsIdentical(File segment1Dir, File segment2Dir) throws Exception {
    ImmutableSegment segment1 = ImmutableSegmentLoader.load(segment1Dir, ReadMode.mmap);
    ImmutableSegment segment2 = ImmutableSegmentLoader.load(segment2Dir, ReadMode.mmap);

    try {
      // Validate metadata
      Assert.assertEquals(segment1.getSegmentMetadata().getTotalDocs(), segment2.getSegmentMetadata().getTotalDocs());
      Assert.assertEquals(segment1.getSegmentMetadata().getAllColumns(), segment2.getSegmentMetadata().getAllColumns());

      // Validate data for each column
      for (String columnName : segment1.getPhysicalColumnNames()) {
        validateColumnData(segment1, segment2, columnName);
      }
    } finally {
      segment1.destroy();
      segment2.destroy();
    }
  }

  private void validateSegmentWithNewColumns(File segmentDir) throws Exception {
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    try {
      // Validate that new columns exist
      Assert.assertTrue(segment.getPhysicalColumnNames().contains(NEW_STRING_COL));
      Assert.assertTrue(segment.getPhysicalColumnNames().contains(NEW_INT_COL));

      // Validate that new columns have default values
      GenericRow row = new GenericRow();
      for (int docId = 0; docId < segment.getSegmentMetadata().getTotalDocs(); docId++) {
        segment.getRecord(docId, row);

        // Check default values
        Assert.assertEquals(row.getValue(NEW_STRING_COL),
            _extendedSchema.getFieldSpecFor(NEW_STRING_COL).getDefaultNullValue());
        Assert.assertEquals(row.getValue(NEW_INT_COL),
            _extendedSchema.getFieldSpecFor(NEW_INT_COL).getDefaultNullValue());
      }
    } finally {
      segment.destroy();
    }
  }

  private void validateColumnData(ImmutableSegment segment1, ImmutableSegment segment2, String columnName) {
    int numDocs = segment1.getSegmentMetadata().getTotalDocs();

    GenericRow row1 = new GenericRow();
    GenericRow row2 = new GenericRow();

    for (int docId = 0; docId < numDocs; docId++) {
      segment1.getRecord(docId, row1);
      segment2.getRecord(docId, row2);

      Object value1 = row1.getValue(columnName);
      Object value2 = row2.getValue(columnName);

      Assert.assertEquals(value1, value2,
              String.format("Column %s differs at docId %d: %s vs %s", columnName, docId, value1, value2));
    }
  }

  private List<GenericRow> generateTestData(int numRows) {
    List<GenericRow> data = new ArrayList<>();

    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();
      row.putValue(STRING_COL_1, "string1_" + i);
      row.putValue(STRING_COL_2, "string2_" + (i % 10));
      row.putValue(INT_COL_1, i);
      row.putValue(INT_COL_2, i * 2);
      row.putValue(LONG_COL, (long) i * 3);
      row.putValue(FLOAT_COL, (float) i * 1.5);
      row.putValue(DOUBLE_COL, (double) i * 2.5);
      row.putValue(BIG_DECIMAL_COL, new BigDecimal(i + ".123"));
      row.putValue(BYTES_COL, ("bytes_" + i).getBytes());
      row.putValue(TIME_COL, System.currentTimeMillis() + i);
      row.putValue(MV_INT_COL, new Object[]{i, i + 1, i + 2});
      row.putValue(MV_STRING_COL, new Object[]{"mv1_" + i, "mv2_" + i, "mv3_" + (i % 3)});

      data.add(row);
    }

    return data;
  }

  /**
   * Simple test record reader for the test data.
   */
  private static class TestRecordReader implements RecordReader {
    private final List<GenericRow> _data;
    private int _currentIndex = 0;

    public TestRecordReader(List<GenericRow> data) {
      _data = data;
    }

    @Override
    public void init(File dataFile, @Nullable Set<String> fieldsToRead,
                     @Nullable RecordReaderConfig recordReaderConfig) {
      _currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
      return _currentIndex < _data.size();
    }

    @Override
    public GenericRow next() {
      return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) {
      if (!hasNext()) {
        throw new IllegalStateException("No more records");
      }

      GenericRow sourceRow = _data.get(_currentIndex++);
      reuse.clear();

      for (Map.Entry<String, Object> entry : sourceRow.getFieldToValueMap().entrySet()) {
        reuse.putValue(entry.getKey(), entry.getValue());
      }

      return reuse;
    }

    @Override
    public void rewind() {
      _currentIndex = 0;
    }

    @Override
    public void close() {
      // No-op
    }
  }
}
