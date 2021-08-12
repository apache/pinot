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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.common.utils.StringUtil;
import org.apache.pinot.segment.local.loader.LocalSegmentDirectoryLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.readers.forward.BaseChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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
    schema.addField(new DimensionFieldSpec(INT_COLUMN, DataType.INT, true));
    schema.addField(new DimensionFieldSpec(LONG_COLUMN, DataType.LONG, true));
    schema.addField(new DimensionFieldSpec(FLOAT_COLUMN, DataType.FLOAT, true));
    schema.addField(new DimensionFieldSpec(DOUBLE_COLUMN, DataType.DOUBLE, true));
    schema.addField(new DimensionFieldSpec(STRING_COLUMN, DataType.STRING, true));

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

    _random = new Random(System.nanoTime());
    _recordReader = buildIndex(tableConfig, schema);
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
    testFixedLengthRawIndexCreator(INT_COLUMN, DataType.INT);
  }

  /**
   * Test for long raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testLongRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(LONG_COLUMN, DataType.LONG);
  }

  /**
   * Test for float raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testFloatRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(FLOAT_COLUMN, DataType.FLOAT);
  }

  /**
   * Test for double raw index creator.
   * Compares values read from the raw index against expected value.
   * @throws Exception
   */
  @Test
  public void testDoubleRawIndexCreator()
      throws Exception {
    testFixedLengthRawIndexCreator(DOUBLE_COLUMN, DataType.DOUBLE);
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
    try (VarByteChunkSVForwardIndexReader rawIndexReader = new VarByteChunkSVForwardIndexReader(indexBuffer,
        DataType.STRING);
        BaseChunkSVForwardIndexReader.ChunkReaderContext readerContext = rawIndexReader.createContext()) {
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        Assert.assertEquals(rawIndexReader.getString(row, readerContext), expectedRow.getValue(STRING_COLUMN));
      }
    }
  }

  /**
   * Helper method to perform actual tests for a given column.
   *
   * @param column Column for which to perform the test
   * @param dataType Data type of the column
   * @throws Exception
   */
  private void testFixedLengthRawIndexCreator(String column, DataType dataType)
      throws Exception {
    PinotDataBuffer indexBuffer = getIndexBufferForColumn(column);
    try (FixedByteChunkSVForwardIndexReader rawIndexReader = new FixedByteChunkSVForwardIndexReader(indexBuffer,
        dataType); BaseChunkSVForwardIndexReader.ChunkReaderContext readerContext = rawIndexReader.createContext()) {
      _recordReader.rewind();
      for (int row = 0; row < NUM_ROWS; row++) {
        GenericRow expectedRow = _recordReader.next();
        Assert.assertEquals(readValueFromIndex(rawIndexReader, readerContext, row), expectedRow.getValue(column));
      }
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
  private RecordReader buildIndex(TableConfig tableConfig, Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
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

    RecordReader recordReader = new GenericRowRecordReader(rows);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();
    Map<String, Object> props = new HashMap<>();
    props.put(LocalSegmentDirectoryLoader.READ_MODE_KEY, ReadMode.mmap.toString());
    _segmentDirectory = SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(driver.getOutputDirectory().toURI(), new PinotConfiguration(props));
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
  public static Object getRandomValue(Random random, DataType dataType) {
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
        return StringUtil
            .sanitizeStringValue(RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH)), Integer.MAX_VALUE);
      default:
        throw new UnsupportedOperationException("Unsupported data type for random value generator: " + dataType);
    }
  }

  /**
   * Helper method to reader value for the given row.
   *
   * @param rawIndexReader Index reader
   * @param readerContext Reader context
   * @param docId Document id
   * @return Value read from index
   */
  private Object readValueFromIndex(FixedByteChunkSVForwardIndexReader rawIndexReader,
      BaseChunkSVForwardIndexReader.ChunkReaderContext readerContext, int docId) {
    switch (rawIndexReader.getValueType()) {
      case INT:
        return rawIndexReader.getInt(docId, readerContext);
      case LONG:
        return rawIndexReader.getLong(docId, readerContext);
      case FLOAT:
        return rawIndexReader.getFloat(docId, readerContext);
      case DOUBLE:
        return rawIndexReader.getDouble(docId, readerContext);
      default:
        throw new IllegalArgumentException(
            "Illegal data type for fixed width raw index reader: " + rawIndexReader.getValueType());
    }
  }
}
