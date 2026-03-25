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
package org.apache.pinot.segment.local.segment.index.creator;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexReaderFactory;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.store.SegmentLocalFSDirectory;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.StringUtil;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end test for V6 two-stream forward index format.
 * Creates a segment with rawIndexWriterVersion=6 via FieldConfig properties,
 * then reads back all columns and verifies correctness.
 */
public class RawIndexCreatorV6Test implements PinotBuffersAfterClassCheckRule {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), RawIndexCreatorV6Test.class.getSimpleName());

  private static final String RAW_TABLE_NAME = "testTableV6";
  private static final String SEGMENT_NAME = "testSegmentV6";
  private static final int NUM_ROWS = 5003;
  private static final int MAX_STRING_LENGTH = 101;

  private static final String STRING_COLUMN = "stringColumn";
  private static final String BYTES_COLUMN = "bytesColumn";
  private static final String INT_MV_COLUMN = "intMVColumn";
  private static final String LONG_MV_COLUMN = "longMVColumn";
  private static final String FLOAT_MV_COLUMN = "floatMVColumn";
  private static final String DOUBLE_MV_COLUMN = "doubleMVColumn";
  private static final String STRING_MV_COLUMN = "stringMVColumn";
  private static final String BYTES_MV_COLUMN = "bytesMVColumn";

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(STRING_COLUMN, DataType.STRING)
      .addSingleValueDimension(BYTES_COLUMN, DataType.BYTES)
      .addMultiValueDimension(INT_MV_COLUMN, DataType.INT)
      .addMultiValueDimension(LONG_MV_COLUMN, DataType.LONG)
      .addMultiValueDimension(FLOAT_MV_COLUMN, DataType.FLOAT)
      .addMultiValueDimension(DOUBLE_MV_COLUMN, DataType.DOUBLE)
      .addMultiValueDimension(STRING_MV_COLUMN, DataType.STRING)
      .addMultiValueDimension(BYTES_MV_COLUMN, DataType.BYTES)
      .build();
  //@formatter:on

  private static final Random RANDOM = new Random(42);

  private RecordReader _recordReader;
  private SegmentDirectory _segmentDirectory;
  private SegmentDirectory.Reader _segmentReader;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    _recordReader = buildIndex();
    _segmentDirectory = new SegmentLocalFSDirectory(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.mmap);
    _segmentReader = _segmentDirectory.createReader();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _segmentReader.close();
    _segmentDirectory.close();
    _recordReader.close();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testStringSVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(STRING_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.STRING, true);
        ForwardIndexReaderContext context = reader.createContext()) {
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        Assert.assertEquals(reader.getString(row, context), expectedRow.getValue(STRING_COLUMN));
      }
    }
  }

  @Test
  public void testBytesSVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(BYTES_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.BYTES, true);
        ForwardIndexReaderContext context = reader.createContext()) {
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        Assert.assertEquals(reader.getBytes(row, context), (byte[]) expectedRow.getValue(BYTES_COLUMN));
      }
    }
  }

  @Test
  public void testIntMVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(INT_MV_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.INT, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      int maxMV = _segmentDirectory.getSegmentMetadata()
          .getColumnMetadataFor(INT_MV_COLUMN).getMaxNumberOfMultiValues();
      int[] valueBuffer = new int[maxMV];
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        int length = reader.getIntMV(row, valueBuffer, context);
        int[] actual = Arrays.copyOf(valueBuffer, length);
        Object[] expected = (Object[]) expectedRow.getValue(INT_MV_COLUMN);
        if (expected == null || expected.length == 0) {
          expected = new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_INT};
        }
        Assert.assertEquals(actual.length, expected.length, "Row " + row);
        for (int i = 0; i < actual.length; i++) {
          Assert.assertEquals(actual[i], ((Number) expected[i]).intValue());
        }
      }
    }
  }

  @Test
  public void testLongMVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(LONG_MV_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.LONG, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      int maxMV = _segmentDirectory.getSegmentMetadata()
          .getColumnMetadataFor(LONG_MV_COLUMN).getMaxNumberOfMultiValues();
      long[] valueBuffer = new long[maxMV];
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        int length = reader.getLongMV(row, valueBuffer, context);
        long[] actual = Arrays.copyOf(valueBuffer, length);
        Object[] expected = (Object[]) expectedRow.getValue(LONG_MV_COLUMN);
        if (expected == null || expected.length == 0) {
          expected = new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_LONG};
        }
        Assert.assertEquals(actual.length, expected.length, "Row " + row);
        for (int i = 0; i < actual.length; i++) {
          Assert.assertEquals(actual[i], ((Number) expected[i]).longValue());
        }
      }
    }
  }

  @Test
  public void testFloatMVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(FLOAT_MV_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.FLOAT, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      int maxMV = _segmentDirectory.getSegmentMetadata()
          .getColumnMetadataFor(FLOAT_MV_COLUMN).getMaxNumberOfMultiValues();
      float[] valueBuffer = new float[maxMV];
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        int length = reader.getFloatMV(row, valueBuffer, context);
        float[] actual = Arrays.copyOf(valueBuffer, length);
        Object[] expected = (Object[]) expectedRow.getValue(FLOAT_MV_COLUMN);
        if (expected == null || expected.length == 0) {
          expected = new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_FLOAT};
        }
        Assert.assertEquals(actual.length, expected.length, "Row " + row);
        for (int i = 0; i < actual.length; i++) {
          Assert.assertEquals(actual[i], ((Number) expected[i]).floatValue());
        }
      }
    }
  }

  @Test
  public void testDoubleMVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(DOUBLE_MV_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.DOUBLE, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      int maxMV = _segmentDirectory.getSegmentMetadata()
          .getColumnMetadataFor(DOUBLE_MV_COLUMN).getMaxNumberOfMultiValues();
      double[] valueBuffer = new double[maxMV];
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        int length = reader.getDoubleMV(row, valueBuffer, context);
        double[] actual = Arrays.copyOf(valueBuffer, length);
        Object[] expected = (Object[]) expectedRow.getValue(DOUBLE_MV_COLUMN);
        if (expected == null || expected.length == 0) {
          expected = new Object[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_DOUBLE};
        }
        Assert.assertEquals(actual.length, expected.length, "Row " + row);
        for (int i = 0; i < actual.length; i++) {
          Assert.assertEquals(actual[i], ((Number) expected[i]).doubleValue());
        }
      }
    }
  }

  @Test
  public void testStringMVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(STRING_MV_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.STRING, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      int maxMV = _segmentDirectory.getSegmentMetadata()
          .getColumnMetadataFor(STRING_MV_COLUMN).getMaxNumberOfMultiValues();
      String[] valueBuffer = new String[maxMV];
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        int length = reader.getStringMV(row, valueBuffer, context);
        String[] actual = Arrays.copyOf(valueBuffer, length);
        Object[] expected = (Object[]) expectedRow.getValue(STRING_MV_COLUMN);
        if (expected == null || expected.length == 0) {
          expected = new String[]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING};
        }
        Assert.assertEquals(actual.length, expected.length, "Row " + row);
        for (int i = 0; i < actual.length; i++) {
          Assert.assertEquals(actual[i], expected[i]);
        }
      }
    }
  }

  @Test
  public void testBytesMVColumn()
      throws Exception {
    PinotDataBuffer indexBuffer = _segmentReader.getIndexFor(BYTES_MV_COLUMN, StandardIndexes.forward());
    try (ForwardIndexReader reader = ForwardIndexReaderFactory.getInstance()
        .createRawIndexReader(indexBuffer, DataType.BYTES, false);
        ForwardIndexReaderContext context = reader.createContext()) {
      int maxMV = _segmentDirectory.getSegmentMetadata()
          .getColumnMetadataFor(BYTES_MV_COLUMN).getMaxNumberOfMultiValues();
      byte[][] valueBuffer = new byte[maxMV][];
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        int length = reader.getBytesMV(row, valueBuffer, context);
        byte[][] actual = Arrays.copyOf(valueBuffer, length);
        Object[] expected = (Object[]) expectedRow.getValue(BYTES_MV_COLUMN);
        if (expected == null || expected.length == 0) {
          expected = new byte[][]{FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_BYTES};
        }
        Assert.assertEquals(actual.length, expected.length, "Row " + row);
        for (int i = 0; i < actual.length; i++) {
          Assert.assertTrue(Arrays.equals(actual[i], (byte[]) expected[i]));
        }
      }
    }
  }

  private RecordReader buildIndex()
      throws Exception {
    // Build FieldConfig list with rawIndexWriterVersion=6 and ZSTANDARD compression for all columns
    List<String> allColumns = SCHEMA.getDimensionNames();
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldConfig.RAW_INDEX_WRITER_VERSION, "6");

    List<FieldConfig> fieldConfigs = allColumns.stream()
        .map(col -> new FieldConfig.Builder(col)
            .withEncodingType(FieldConfig.EncodingType.RAW)
            .withCompressionCodec(FieldConfig.CompressionCodec.ZSTANDARD)
            .withProperties(properties)
            .build())
        .collect(Collectors.toList());

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(RAW_TABLE_NAME)
        .setNoDictionaryColumns(allColumns)
        .setFieldConfigList(fieldConfigs)
        .build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, SCHEMA);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();
      for (FieldSpec fieldSpec : SCHEMA.getAllFieldSpecs()) {
        if (fieldSpec.isSingleValueField()) {
          row.putValue(fieldSpec.getName(), getRandomValue(RANDOM, fieldSpec.getDataType()));
        } else {
          int length = RANDOM.nextInt(50);
          Object[] values = new Object[length];
          for (int j = 0; j < length; j++) {
            values[j] = getRandomValue(RANDOM, fieldSpec.getDataType());
          }
          row.putValue(fieldSpec.getName(), values);
        }
      }
      rows.add(row);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    recordReader.rewind();
    return recordReader;
  }

  private static Object getRandomValue(Random random, DataType dataType) {
    switch (dataType) {
      case INT:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case FLOAT:
        return random.nextFloat();
      case DOUBLE:
        return random.nextDouble();
      case STRING:
        return StringUtil.sanitizeStringValue(
            RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH)), Integer.MAX_VALUE);
      case BYTES:
        return StringUtil.sanitizeStringValue(
            RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH)), Integer.MAX_VALUE).getBytes();
      default:
        throw new UnsupportedOperationException("Unsupported data type: " + dataType);
    }
  }
}
