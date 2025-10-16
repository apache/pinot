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
import java.util.Random;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReaderFactory;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderConfig;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;


/**
 * Base class for columnar segment building tests containing common setup and utility methods.
 *
 * <p>This base class provides:
 * <ul>
 *   <li>Common test data and schema setup</li>
 *   <li>Segment creation utility methods</li>
 *   <li>Validation utility methods</li>
 *   <li>Test data generation</li>
 * </ul>
 */
public abstract class ColumnarSegmentBuildingTestBase {
  protected static final String TEMP_DIR = System.getProperty("java.io.tmpdir");
  protected static final String TABLE_NAME = "testTable";
  protected static final String SEGMENT_NAME = "testSegment";

  // Test columns
  protected static final String STRING_COL_1 = "stringCol1";
  protected static final String STRING_COL_2 = "stringCol2";
  protected static final String INT_COL_1 = "intCol1";
  protected static final String INT_COL_2 = "intCol2";
  protected static final String LONG_COL = "longCol";
  protected static final String FLOAT_COL = "floatCol";
  protected static final String DOUBLE_COL = "doubleCol";
  protected static final String BIG_DECIMAL_COL = "bigDecimalCol";
  protected static final String BYTES_COL = "bytesCol";
  protected static final String TIME_COL = "timeCol";
  protected static final String MV_INT_COL = "mvIntCol";
  protected static final String MV_STRING_COL = "mvStringCol";

  // New column for testing default value handling
  protected static final String NEW_STRING_COL = "newStringCol";
  protected static final String NEW_INT_COL = "newIntCol";
  protected static final String NEW_MV_LONG_COL = "newMvLongCol";

  protected File _tempDir;
  protected Schema _originalSchema;
  protected Schema _extendedSchema; // Schema with additional columns
  protected TableConfig _tableConfig;
  protected List<GenericRow> _testData;

  @BeforeClass
  public void setUp()
      throws IOException {
    _tempDir = new File(TEMP_DIR, getClass().getSimpleName());
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
        .addMultiValueDimension(NEW_MV_LONG_COL, FieldSpec.DataType.LONG)
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
  public void tearDown()
      throws IOException {
    FileUtils.deleteQuietly(_tempDir);
  }

  protected File createRowMajorSegment()
      throws Exception {
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

  protected File createColumnarSegment(File sourceSegmentDir)
      throws Exception {
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

  protected File createColumnarSegmentWithNewColumns(File sourceSegmentDir)
      throws Exception {
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

  protected File createRowMajorSegmentWithExtendedSchema(File originalSegmentDir)
      throws Exception {
    File outputDir = new File(_tempDir, "rowMajorSegmentWithExtendedSchema");
    FileUtils.deleteQuietly(outputDir);
    outputDir.mkdirs();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, _extendedSchema);
    config.setOutDir(outputDir.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME + "_rowMajorExtended");

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();

    // Use PinotSegmentRecordReader to read from the original segment, similar to RefreshSegmentTaskExecutor
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(originalSegmentDir, null, null);
      driver.init(config, recordReader);
      driver.build();
    }

    return new File(outputDir, SEGMENT_NAME + "_rowMajorExtended");
  }

  protected void validateSegmentsIdentical(File segment1Dir, File segment2Dir)
      throws Exception {
    ImmutableSegment segment1 = ImmutableSegmentLoader.load(segment1Dir, ReadMode.mmap);
    ImmutableSegment segment2 = ImmutableSegmentLoader.load(segment2Dir, ReadMode.mmap);

    try {
      // Validate basic segment metadata
      Assert.assertEquals(segment1.getSegmentMetadata().getTotalDocs(), segment2.getSegmentMetadata().getTotalDocs());
      Assert.assertEquals(segment1.getSegmentMetadata().getAllColumns(), segment2.getSegmentMetadata().getAllColumns());

      // Validate column metadata and statistics for each column
      for (String columnName : segment1.getPhysicalColumnNames()) {
        validateColumnMetadata(segment1, segment2, columnName);
        validateColumnData(segment1, segment2, columnName);
      }
    } finally {
      segment1.destroy();
      segment2.destroy();
    }
  }

  protected void validateColumnMetadata(ImmutableSegment segment1, ImmutableSegment segment2, String columnName) {
    ColumnMetadata metadata1 = segment1.getSegmentMetadata().getColumnMetadataFor(columnName);
    ColumnMetadata metadata2 = segment2.getSegmentMetadata().getColumnMetadataFor(columnName);

    Assert.assertNotNull(metadata1, "Column metadata missing for " + columnName + " in segment1");
    Assert.assertNotNull(metadata2, "Column metadata missing for " + columnName + " in segment2");

    // Validate basic column properties
    Assert.assertEquals(metadata1.getDataType(), metadata2.getDataType(),
        "Data type mismatch for column " + columnName);
    Assert.assertEquals(metadata1.isSingleValue(), metadata2.isSingleValue(),
        "Single value flag mismatch for column " + columnName);
    Assert.assertEquals(metadata1.getFieldType(), metadata2.getFieldType(),
        "Field type mismatch for column " + columnName);
    Assert.assertEquals(metadata1.getTotalDocs(), metadata2.getTotalDocs(),
        "Total docs mismatch for column " + columnName);

    // Validate cardinality and dictionary properties
    Assert.assertEquals(metadata1.getCardinality(), metadata2.getCardinality(),
        "Cardinality mismatch for column " + columnName);
    Assert.assertEquals(metadata1.hasDictionary(), metadata2.hasDictionary(),
        "Dictionary flag mismatch for column " + columnName);
    Assert.assertEquals(metadata1.getColumnMaxLength(), metadata2.getColumnMaxLength(),
        "Column max length mismatch for column " + columnName);

    // Validate sorting and indexing properties
    Assert.assertEquals(metadata1.isSorted(), metadata2.isSorted(),
        "Sorted flag mismatch for column " + columnName);
    Assert.assertEquals(metadata1.isAutoGenerated(), metadata2.isAutoGenerated(),
        "Auto-generated flag mismatch for column " + columnName);

    // Validate multi-value properties
    if (!metadata1.isSingleValue()) {
      Assert.assertEquals(metadata1.getMaxNumberOfMultiValues(), metadata2.getMaxNumberOfMultiValues(),
          "Max number of multi-values mismatch for column " + columnName);
      Assert.assertEquals(metadata1.getTotalNumberOfEntries(), metadata2.getTotalNumberOfEntries(),
          "Total number of entries mismatch for column " + columnName);
    }

    // Validate min/max values if available
    Comparable<?> minValue1 = metadata1.getMinValue();
    Comparable<?> minValue2 = metadata2.getMinValue();
    Comparable<?> maxValue1 = metadata1.getMaxValue();
    Comparable<?> maxValue2 = metadata2.getMaxValue();

    if (minValue1 != null && minValue2 != null) {
      Assert.assertEquals(minValue1, minValue2,
          "Min value mismatch for column " + columnName);
    }
    if (maxValue1 != null && maxValue2 != null) {
      Assert.assertEquals(maxValue1, maxValue2,
          "Max value mismatch for column " + columnName);
    }

    // Validate bits per element for dictionary encoded columns
    if (metadata1.hasDictionary() && metadata2.hasDictionary()) {
      Assert.assertEquals(metadata1.getBitsPerElement(), metadata2.getBitsPerElement(),
          "Bits per element mismatch for column " + columnName);
    }
  }

  protected void validateSegmentWithNewColumns(File segmentDir)
      throws Exception {
    ImmutableSegment segment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    try {
      // Validate that new columns exist
      Assert.assertTrue(segment.getPhysicalColumnNames().contains(NEW_STRING_COL));
      Assert.assertTrue(segment.getPhysicalColumnNames().contains(NEW_INT_COL));
      Assert.assertTrue(segment.getPhysicalColumnNames().contains(NEW_MV_LONG_COL));

      // Validate that new columns have default values
      GenericRow row = new GenericRow();
      for (int docId = 0; docId < segment.getSegmentMetadata().getTotalDocs(); docId++) {
        segment.getRecord(docId, row);

        // Check default values
        Assert.assertEquals(row.getValue(NEW_STRING_COL),
            _extendedSchema.getFieldSpecFor(NEW_STRING_COL).getDefaultNullValue());
        Assert.assertEquals(row.getValue(NEW_INT_COL),
            _extendedSchema.getFieldSpecFor(NEW_INT_COL).getDefaultNullValue());

        // For multi-value columns, the default value is wrapped in an array
        Object mvLongValue = row.getValue(NEW_MV_LONG_COL);
        Assert.assertTrue(mvLongValue instanceof Object[], "Multi-value column should return an array");
        Object[] mvLongArray = (Object[]) mvLongValue;
        Assert.assertEquals(mvLongArray.length, 1, "Default multi-value array should have one element");
        Assert.assertEquals(mvLongArray[0],
            _extendedSchema.getFieldSpecFor(NEW_MV_LONG_COL).getDefaultNullValue());
      }
    } finally {
      segment.destroy();
    }
  }

  protected void validateColumnData(ImmutableSegment segment1, ImmutableSegment segment2, String columnName) {
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

  protected List<GenericRow> generateTestData(int numRows) {
    List<GenericRow> data = new ArrayList<>();

    // Use a fixed seed for reproducible test results
    Random random = new Random(42);

    // Null probability - approximately 10% of values will be null for nullable columns
    double nullProbability = 0.1;

    for (int i = 0; i < numRows; i++) {
      GenericRow row = new GenericRow();

      // STRING columns - can be null
      row.putValue(STRING_COL_1, random.nextDouble() < nullProbability ? null : "string1_" + i);
      row.putValue(STRING_COL_2, random.nextDouble() < nullProbability ? null : "string2_" + (i % 10));

      // INT columns - can be null
      row.putValue(INT_COL_1, random.nextDouble() < nullProbability ? null : i);
      row.putValue(INT_COL_2, random.nextDouble() < nullProbability ? null : i * 2);

      // LONG column - can be null
      row.putValue(LONG_COL, random.nextDouble() < nullProbability ? null : (long) i * 3);

      // FLOAT column - can be null
      row.putValue(FLOAT_COL, random.nextDouble() < nullProbability ? null : (float) i * 1.5);

      // DOUBLE column - can be null
      row.putValue(DOUBLE_COL, random.nextDouble() < nullProbability ? null : (double) i * 2.5);

      // BIG_DECIMAL column - can be null
      row.putValue(BIG_DECIMAL_COL, random.nextDouble() < nullProbability ? null : new BigDecimal(i + ".123"));

      // BYTES column - can be null
      row.putValue(BYTES_COL, random.nextDouble() < nullProbability ? null : ("bytes_" + i).getBytes());

      // TIME column - typically not null for time columns, but we can test it
      // Use a deterministic timestamp based on the row index for consistent results
      long baseTimestamp = 1700000000000L; // Fixed base timestamp
      row.putValue(TIME_COL, random.nextDouble() < nullProbability ? null : baseTimestamp + i);

      // Multi-value columns - can be null (entire array is null, not individual elements)
      row.putValue(MV_INT_COL, random.nextDouble() < nullProbability ? null : new Object[]{i, i + 1, i + 2});
      row.putValue(MV_STRING_COL, random.nextDouble() < nullProbability ? null
          : new Object[]{"mv1_" + i, "mv2_" + i, "mv3_" + (i % 3)});

      data.add(row);
    }

    return data;
  }

  /**
   * Simple test record reader for the test data.
   */
  protected static class TestRecordReader implements RecordReader {
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
