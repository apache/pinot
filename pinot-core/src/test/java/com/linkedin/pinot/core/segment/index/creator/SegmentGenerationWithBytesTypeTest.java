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

import com.google.common.primitives.Ints;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.utils.primitive.ByteArray;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Class for testing segment generation with byte[] data type.
 */
public class SegmentGenerationWithBytesTypeTest {
  private static final int NUM_ROWS = 10001;
  private static final int FIXED_BYTE_LENGTH = 53;
  private static final int MAX_VARIABLE_BYTES_LENGTH = 101;
  private static final int NUM_SORTED_VALUES = 1001;

  private static final String SEGMENT_DIR_NAME =
      System.getProperty("java.io.tmpdir") + File.separator + "bytesTypeTest";
  private static final String SEGMENT_NAME = "testSegment";

  private static final String FIXED_BYTE_SORTED_COLUMN = "sortedColumn";
  private static final String FIXED_BYTES_UNSORTED_COLUMN = "fixedBytes";
  private static final String FIXED_BYTES_NO_DICT_COLUMN = "fixedBytesNoDict";
  private static final String VARIABLE_BYTES_COLUMN = "variableBytes";

  private Random _random;
  private RecordReader _recordReader;
  private Schema _schema;
  private ImmutableSegment _segment;

  /**
   * Setup to build a segment with raw indexes (no-dictionary) of various data types.
   *
   * @throws Exception
   */
  @BeforeClass
  public void setup()
      throws Exception {

    _schema = new Schema();
    _schema.addField(new DimensionFieldSpec(FIXED_BYTE_SORTED_COLUMN, FieldSpec.DataType.BYTES, true));
    _schema.addField(new DimensionFieldSpec(FIXED_BYTES_UNSORTED_COLUMN, FieldSpec.DataType.BYTES, true));
    _schema.addField(new DimensionFieldSpec(FIXED_BYTES_NO_DICT_COLUMN, FieldSpec.DataType.BYTES, true));
    _schema.addField(new DimensionFieldSpec(VARIABLE_BYTES_COLUMN, FieldSpec.DataType.BYTES, true));

    _random = new Random(System.nanoTime());
    _recordReader = buildIndex(_schema);
    _segment = ImmutableSegmentLoader.load(new File(SEGMENT_DIR_NAME, SEGMENT_NAME), ReadMode.heap);
  }

  /**
   * Clean up after test
   */
  @AfterClass
  public void cleanup()
      throws IOException {
    _recordReader.close();
    _segment.destroy();
    FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
  }

  @Test
  public void test()
      throws Exception {
    PinotSegmentRecordReader pinotReader = new PinotSegmentRecordReader(new File(SEGMENT_DIR_NAME, SEGMENT_NAME));

    _recordReader.rewind();
    while (pinotReader.hasNext()) {
      GenericRow expectedRow = _recordReader.next();
      GenericRow actualRow = pinotReader.next();

      for (String column : _schema.getColumnNames()) {
        byte[] actual = (byte[]) actualRow.getValue(column);
        byte[] expected = (byte[]) expectedRow.getValue(column);

        if (ByteArray.compare(actual, expected) != 0) {
          Assert.assertEquals(actualRow.getValue(column), expectedRow.getValue(column));
        }
      }
    }

    // Ensure both record readers are exhausted, ie same number of rows.
    Assert.assertTrue(!_recordReader.hasNext());
    pinotReader.close();
  }

  @Test
  public void testMetadata() {
    Assert.assertTrue(_segment.getDataSource(FIXED_BYTE_SORTED_COLUMN).getDataSourceMetadata().isSorted());
    Assert.assertFalse(_segment.getSegmentMetadata().hasDictionary(FIXED_BYTES_NO_DICT_COLUMN));
  }

  @Test
  public void testDictionary()
      throws Exception {
    ImmutableDictionaryReader dictionary = _segment.getDictionary(FIXED_BYTE_SORTED_COLUMN);
    Assert.assertEquals(dictionary.length(), NUM_SORTED_VALUES);

    // Test dictionary indexing.
    for (int i = 0; i < NUM_ROWS; i++) {
      int value = (i * NUM_SORTED_VALUES) / NUM_ROWS;
    // For sorted columns, values are written as 0, 0, 0.., 1, 1, 1...n, n, n
      Assert.assertEquals(dictionary.indexOf(Ints.toByteArray(value)), value % NUM_SORTED_VALUES);
    }

    // Test value not in dictionary.
    Assert.assertEquals(dictionary.indexOf(Ints.toByteArray(NUM_SORTED_VALUES + 1)), -1);
    Assert.assertEquals(dictionary.insertionIndexOf(Ints.toByteArray(NUM_SORTED_VALUES + 1)),
        -(dictionary.length() + 1));

    int[] dictIds = new int[dictionary.length()];
    for (int i = 0; i < dictIds.length; i++) {
      dictIds[i] = i;
    }

    byte[][] values = new byte[dictIds.length][];
    dictionary.readBytesValues(dictIds, 0, dictIds.length, values,0);
    for (int expected = 0; expected < values.length; expected++) {
      int actual = ByteBuffer.wrap(values[expected]).asIntBuffer().get();
      Assert.assertEquals(actual, expected);
    }
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

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(Arrays.asList(FIXED_BYTES_NO_DICT_COLUMN));

    List<GenericRow> rows = new ArrayList<>(NUM_ROWS);
    for (int i = 0; i < NUM_ROWS; i++) {
      HashMap<String, Object> map = new HashMap<>();

      // Set the value for fixed-byte sorted column.
      map.put(FIXED_BYTE_SORTED_COLUMN, Ints.toByteArray((i * NUM_SORTED_VALUES) / NUM_ROWS));

      // Set the value for fixed-byte unsorted column.
      byte[] fixedBytes = new byte[FIXED_BYTE_LENGTH];
      _random.nextBytes(fixedBytes);
      map.put(FIXED_BYTES_UNSORTED_COLUMN, fixedBytes);

      // Set the value for fixed-byte no-dictionary column.
      map.put(FIXED_BYTES_NO_DICT_COLUMN, fixedBytes);

      // Set the value fo variable length column. Ensure at least one zero-length byte[].
      int length = (i == 0) ? 0 : _random.nextInt(MAX_VARIABLE_BYTES_LENGTH);
      byte[] varBytes = new byte[length];
      _random.nextBytes(varBytes);
      map.put(VARIABLE_BYTES_COLUMN, varBytes);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    SegmentDirectory.createFromLocalFS(driver.getOutputDirectory(), ReadMode.mmap);
    recordReader.rewind();
    return recordReader;
  }
}
