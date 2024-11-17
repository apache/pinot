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

import com.google.common.primitives.Ints;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Class for testing segment generation with byte[] data type.
 */
public class SegmentGenerationWithBytesTypeTest {
  private static final File TEMP_DIR =
      new File(FileUtils.getTempDirectory(), SegmentGenerationWithBytesTypeTest.class.getSimpleName());

  private static final String RAW_TABLE_NAME = "testTable";
  private static final String SEGMENT_NAME = "testSegment";
  private static final int NUM_ROWS = 10001;
  private static final int FIXED_BYTE_LENGTH = 53;
  private static final int MAX_VARIABLE_BYTES_LENGTH = 101;
  private static final int NUM_SORTED_VALUES = 1001;

  private static final String FIXED_BYTE_SORTED_COLUMN = "sortedColumn";
  private static final String FIXED_BYTES_UNSORTED_COLUMN = "fixedBytes";
  private static final String FIXED_BYTES_NO_DICT_COLUMN = "fixedBytesNoDict";
  private static final String VARIABLE_BYTES_COLUMN = "variableBytes";

  //@formatter:off
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(RAW_TABLE_NAME)
      .addSingleValueDimension(FIXED_BYTE_SORTED_COLUMN, DataType.BYTES)
      .addSingleValueDimension(FIXED_BYTES_UNSORTED_COLUMN, DataType.BYTES)
      .addSingleValueDimension(FIXED_BYTES_NO_DICT_COLUMN, DataType.BYTES)
      .addSingleValueDimension(VARIABLE_BYTES_COLUMN, DataType.BYTES)
      .build();
  //@formatter:on
  private static final TableConfig TABLE_CONFIG = new TableConfigBuilder(TableType.OFFLINE).setTableName(RAW_TABLE_NAME)
      .setNoDictionaryColumns(List.of(FIXED_BYTES_NO_DICT_COLUMN)).build();
  private static final Random RANDOM = new Random();

  private List<GenericRow> _rows;
  private ImmutableSegment _segment;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);
    _rows = generateRows();
    buildSegment();
    _segment = ImmutableSegmentLoader.load(new File(TEMP_DIR, SEGMENT_NAME), ReadMode.heap);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _segment.destroy();
    FileUtils.deleteQuietly(TEMP_DIR);
  }

  @Test
  public void testRecords()
      throws Exception {
    try (PinotSegmentRecordReader recordReader = new PinotSegmentRecordReader()) {
      recordReader.init(_segment);
      for (int i = 0; i < NUM_ROWS; i++) {
        assertEquals(recordReader.next(), _rows.get(i));
      }
      assertFalse(recordReader.hasNext());
    }
  }

  @Test
  public void testMetadata() {
    assertTrue(_segment.getDataSource(FIXED_BYTE_SORTED_COLUMN).getDataSourceMetadata().isSorted());
    assertFalse(_segment.getSegmentMetadata().getColumnMetadataFor(FIXED_BYTES_NO_DICT_COLUMN).hasDictionary());
  }

  @Test
  public void testDictionary() {
    BaseImmutableDictionary dictionary = (BaseImmutableDictionary) _segment.getDictionary(FIXED_BYTE_SORTED_COLUMN);
    assertEquals(dictionary.length(), NUM_SORTED_VALUES);

    // Test dictionary indexing.
    for (int i = 0; i < NUM_ROWS; i++) {
      int value = (i * NUM_SORTED_VALUES) / NUM_ROWS;
      // For sorted columns, values are written as 0, 0, 0.., 1, 1, 1...n, n, n
      assertEquals(dictionary.indexOf(BytesUtils.toHexString(Ints.toByteArray(value))), value % NUM_SORTED_VALUES);
    }

    // Test value not in dictionary.
    assertEquals(dictionary.indexOf(BytesUtils.toHexString(Ints.toByteArray(NUM_SORTED_VALUES + 1))), -1);
    assertEquals(dictionary.insertionIndexOf(BytesUtils.toHexString(Ints.toByteArray(NUM_SORTED_VALUES + 1))),
        -(NUM_SORTED_VALUES + 1));

    int[] dictIds = new int[NUM_SORTED_VALUES];
    for (int i = 0; i < NUM_SORTED_VALUES; i++) {
      dictIds[i] = i;
    }

    byte[][] values = new byte[NUM_SORTED_VALUES][];
    dictionary.readBytesValues(dictIds, NUM_SORTED_VALUES, values);
    for (int expected = 0; expected < NUM_SORTED_VALUES; expected++) {
      int actual = ByteBuffer.wrap(values[expected]).asIntBuffer().get();
      assertEquals(actual, expected);
    }
  }

  private List<GenericRow> generateRows() {
    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      GenericRow row = new GenericRow();

      // Set the value for fixed-byte sorted column.
      row.putValue(FIXED_BYTE_SORTED_COLUMN, Ints.toByteArray((i * NUM_SORTED_VALUES) / NUM_ROWS));

      // Set the value for fixed-byte unsorted column.
      byte[] fixedBytes = new byte[FIXED_BYTE_LENGTH];
      RANDOM.nextBytes(fixedBytes);
      row.putValue(FIXED_BYTES_UNSORTED_COLUMN, fixedBytes);

      // Set the value for fixed-byte no-dictionary column.
      row.putValue(FIXED_BYTES_NO_DICT_COLUMN, fixedBytes);

      // Set the value fo variable length column. Ensure at least one zero-length byte[].
      int length = (i == 0) ? 0 : RANDOM.nextInt(MAX_VARIABLE_BYTES_LENGTH);
      byte[] varBytes = new byte[length];
      RANDOM.nextBytes(varBytes);
      row.putValue(VARIABLE_BYTES_COLUMN, varBytes);

      rows.add(row);
    }
    return rows;
  }

  private void buildSegment()
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(TABLE_CONFIG, SCHEMA);
    config.setOutDir(TEMP_DIR.getAbsolutePath());
    config.setSegmentName(SEGMENT_NAME);

    try (RecordReader recordReader = new GenericRowRecordReader(_rows)) {
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, recordReader);
      driver.build();
    }
  }
}
