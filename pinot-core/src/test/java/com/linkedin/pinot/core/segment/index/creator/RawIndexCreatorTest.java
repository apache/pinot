/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.segment.index.creator;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.StringUtil;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.io.reader.impl.ChunkReaderContext;
import com.linkedin.pinot.core.io.reader.impl.v1.FixedByteChunkSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.VarByteChunkSingleValueReader;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.ColumnIndexType;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Class for testing Raw index creators.
 */
public class RawIndexCreatorTest {
  private static final int NUM_ROWS = 10009;
  private static final int MAX_STRING_LENGTH = 101;

  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "fwdIndexTest";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String INT_COLUMN = "intColumn";
  private static final String LONG_COLUMN = "longColumn";
  private static final String FLOAT_COLUMN = "floatColumn";
  private static final String DOUBLE_COLUMN = "doubleColumn";
  private static final String STRING_COLUMN = "stringColumn";

  Random _random;
  private RecordReader _recordReader;
  SegmentDirectory _segmentDirectory;
  private SegmentDirectory.Reader _segmentReader;

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {

    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(INT_COLUMN, FieldSpec.DataType.INT, true));
    schema.addField(new DimensionFieldSpec(LONG_COLUMN, FieldSpec.DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(FLOAT_COLUMN, FieldSpec.DataType.FLOAT, true));
    schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec(STRING_COLUMN, FieldSpec.DataType.STRING, true));

    _random = new Random(System.nanoTime());
    _recordReader = buildIndex(schema);
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup() {
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  /**
   * Test for int raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testIntRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(INT_COLUMN, FieldSpec.DataType.INT);
  }

  /**
   * Test for long raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testLongRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(LONG_COLUMN, FieldSpec.DataType.LONG);
  }

  /**
   * Test for float raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testFloatRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(FLOAT_COLUMN, FieldSpec.DataType.FLOAT);
  }

  /**
   * Test for double raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testDoubleRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(DOUBLE_COLUMN, FieldSpec.DataType.DOUBLE);
  }

  /**
   * Test for string raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testStringRawIndexCreator()
      throws Exception {
    PinotDataBuffer indexBuffer = getIndexBufferForColumn(STRING_COLUMN);
    VarByteChunkSingleValueReader rawIndexReader = new VarByteChunkSingleValueReader(indexBuffer);

    _recordReader.rewind();
    ChunkReaderContext context = rawIndexReader.createContext();
    for (int row = 0; row < NUM_ROWS; row++) {
      GenericRow expectedRow = _recordReader.next();
      Object expected = expectedRow.getValue(STRING_COLUMN);
      Object actual = rawIndexReader.getString(row, context);
      Assert.assertEquals(actual, expected);
    }
  }

  /**
   * Helper method to perform actual tests for a given column.
   *
   * @param column Column for which to perform the test
   * @param dataType Data type of the column
   * @throws Exception
   */
  private void testFixedLengthRawIndexCreator(String column, FieldSpec.DataType dataType)
      throws Exception {
    PinotDataBuffer indexBuffer = getIndexBufferForColumn(column);

    FixedByteChunkSingleValueReader rawIndexReader = new FixedByteChunkSingleValueReader(indexBuffer);

    _recordReader.rewind();
    for (int row = 0; row < NUM_ROWS; row++) {
      GenericRow expectedRow = _recordReader.next();
      Object expected = expectedRow.getValue(column);

      Object actual;
      actual = readValueFromIndex(rawIndexReader, dataType, row);
      Assert.assertEquals(actual, expected);
    }
  }

  /**
   * Helper method that returns index file name for a given column name.
   *
   * @param column Column name for which to get the index file name
   * @return Name of index file for the given column name
   */
  private PinotDataBuffer getIndexBufferForColumn(String column)
      throws IOException {
    return _segmentReader.getIndexFor(column, ColumnIndexType.FORWARD_INDEX);
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */
  private RecordReader buildIndex(Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setRawIndexCreationColumns(schema.getDimensionNames());

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;

        value = getRandomValue(_random, fieldSpec.getDataType());
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();
    _segmentDirectory = SegmentDirectory.createFromLocalFS(driver.getOutputDirectory(), ReadMode.mmap);
    _segmentReader = _segmentDirectory.createReader();
    recordReader.rewind();
    return recordReader;
  }

  /**
   * Helper method that generates a random value for a given data type.
   *
   * @param dataType Data type for which to generate the random value
   * @return Random value for the data type
   */
  public static Object getRandomValue(Random random, FieldSpec.DataType dataType) {
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
        return StringUtil.removeNullCharacters(RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH)));
      default:
        throw new UnsupportedOperationException("Unsupported data type for random value generator: " + dataType);
    }
  }

  /**
   * Helper method to reader value for the given row.
   *
   * @param rawIndexReader Index reader
   * @param dataType Data type of value to be read
   * @param row Row to read
   * @return Value read from index
   */
  private Object readValueFromIndex(FixedByteChunkSingleValueReader rawIndexReader, FieldSpec.DataType dataType,
      int row) {
    Object actual;
    ChunkReaderContext context = rawIndexReader.createContext();
    switch (dataType) {
      case INT:
        actual = rawIndexReader.getInt(row, context);
        break;

      case LONG:
        actual = rawIndexReader.getLong(row, context);
        break;

      case FLOAT:
        actual = rawIndexReader.getFloat(row, context);
        break;

      case DOUBLE:
        actual = rawIndexReader.getDouble(row, context);
        break;

      default:
        throw new IllegalArgumentException("Illegal data type for fixed width raw index reader: " + dataType);
    }
    return actual;
  }
}
