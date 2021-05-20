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
import com.tdunning.math.stats.TDigest;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.aggregator.PercentileTDigestValueAggregator;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.local.segment.store.LocalSegmentDirectoryLoader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.loader.SegmentDirectoryLoaderRegistry;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
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

  private static final String AVRO_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "tDigestTest";

  private static final String AVRO_NAME = "tDigest.avro";

  private static final String FIXED_BYTE_SORTED_COLUMN = "sortedColumn";
  private static final String FIXED_BYTES_UNSORTED_COLUMN = "fixedBytes";
  private static final String FIXED_BYTES_NO_DICT_COLUMN = "fixedBytesNoDict";
  private static final String VARIABLE_BYTES_COLUMN = "variableBytes";

  private Random _random;
  private RecordReader _recordReader;
  private Schema _schema;
  private TableConfig _tableConfig;
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

    _tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

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
    FileUtils.deleteQuietly(new File(AVRO_DIR_NAME));
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
    Assert.assertFalse(_recordReader.hasNext());
    pinotReader.close();
  }

  @Test
  public void testMetadata() {
    Assert.assertTrue(_segment.getDataSource(FIXED_BYTE_SORTED_COLUMN).getDataSourceMetadata().isSorted());
    Assert.assertFalse(_segment.getSegmentMetadata().hasDictionary(FIXED_BYTES_NO_DICT_COLUMN));
  }

  @Test
  public void testDictionary() {
    BaseImmutableDictionary dictionary = (BaseImmutableDictionary) _segment.getDictionary(FIXED_BYTE_SORTED_COLUMN);
    Assert.assertEquals(dictionary.length(), NUM_SORTED_VALUES);

    // Test dictionary indexing.
    for (int i = 0; i < NUM_ROWS; i++) {
      int value = (i * NUM_SORTED_VALUES) / NUM_ROWS;
      // For sorted columns, values are written as 0, 0, 0.., 1, 1, 1...n, n, n
      Assert
          .assertEquals(dictionary.indexOf(BytesUtils.toHexString(Ints.toByteArray(value))), value % NUM_SORTED_VALUES);
    }

    // Test value not in dictionary.
    Assert.assertEquals(dictionary.indexOf(BytesUtils.toHexString(Ints.toByteArray(NUM_SORTED_VALUES + 1))), -1);
    Assert.assertEquals(dictionary.insertionIndexOf(BytesUtils.toHexString(Ints.toByteArray(NUM_SORTED_VALUES + 1))),
        -(NUM_SORTED_VALUES + 1));

    int[] dictIds = new int[NUM_SORTED_VALUES];
    for (int i = 0; i < NUM_SORTED_VALUES; i++) {
      dictIds[i] = i;
    }

    byte[][] values = new byte[NUM_SORTED_VALUES][];
    dictionary.readBytesValues(dictIds, NUM_SORTED_VALUES, values);
    for (int expected = 0; expected < NUM_SORTED_VALUES; expected++) {
      int actual = ByteBuffer.wrap(values[expected]).asIntBuffer().get();
      Assert.assertEquals(actual, expected);
    }
  }

  /**
   * This test generates an avro with TDigest BYTES data, and tests segment generation.
   */
  @Test
  public void testTDigestAvro()
      throws Exception {
    Schema schema = new Schema();
    schema.addField(new MetricFieldSpec(FIXED_BYTES_UNSORTED_COLUMN, FieldSpec.DataType.BYTES));
    schema.addField(new MetricFieldSpec(VARIABLE_BYTES_COLUMN, FieldSpec.DataType.BYTES));

    List<byte[]> _fixedExpected = new ArrayList<>(NUM_ROWS);
    List<byte[]> _varExpected = new ArrayList<>(NUM_ROWS);

    buildAvro(schema, _fixedExpected, _varExpected);

    IndexSegment segment = buildSegmentFromAvro(schema, AVRO_DIR_NAME, AVRO_NAME, SEGMENT_NAME);
    SegmentMetadata metadata = segment.getSegmentMetadata();

    Assert.assertTrue(metadata.hasDictionary(FIXED_BYTES_UNSORTED_COLUMN));
    Assert.assertTrue(metadata.hasDictionary(VARIABLE_BYTES_COLUMN));

    PinotSegmentRecordReader reader = new PinotSegmentRecordReader(new File(AVRO_DIR_NAME, SEGMENT_NAME));
    GenericRow row = new GenericRow();

    int i = 0;
    while (reader.hasNext()) {
      row = reader.next(row);
      Assert.assertEquals(ByteArray.compare((byte[]) row.getValue(FIXED_BYTES_UNSORTED_COLUMN), _fixedExpected.get(i)),
          0);
      Assert.assertEquals(ByteArray.compare((byte[]) row.getValue(VARIABLE_BYTES_COLUMN), _varExpected.get(i++)), 0);
    }
    segment.destroy();
  }

  /**
   * Helper method to build a segment containing a single valued string column with RAW (no-dictionary) index.
   *
   * @return Array of string values for the rows in the generated index.
   * @throws Exception
   */

  private RecordReader buildIndex(Schema schema)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, schema);

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);
    config.setRawIndexCreationColumns(Collections.singletonList(FIXED_BYTES_NO_DICT_COLUMN));

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

    RecordReader recordReader = new GenericRowRecordReader(rows);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, recordReader);
    driver.build();

    Map<String, Object> props = new HashMap<>();
    props.put(LocalSegmentDirectoryLoader.READ_MODE_KEY, ReadMode.mmap.toString());
    SegmentDirectoryLoaderRegistry.getLocalSegmentDirectoryLoader()
        .load(driver.getOutputDirectory().toURI(), new PinotConfiguration(props));
    recordReader.rewind();
    return recordReader;
  }

  /**
   * Build Avro file containing serialized TDigest bytes.
   *
   * @param schema Schema of data (one fixed and one variable column)
   * @param _fixedExpected Serialized bytes of fixed length column are populated here
   * @param _varExpected Serialized bytes of variable length column are populated here
   * @throws IOException
   */
  private void buildAvro(Schema schema, List<byte[]> _fixedExpected, List<byte[]> _varExpected)
      throws IOException {
    org.apache.avro.Schema avroSchema = AvroUtils.getAvroSchemaFromPinotSchema(schema);

    try (DataFileWriter<GenericData.Record> recordWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {

      if (!new File(AVRO_DIR_NAME).mkdir()) {
        throw new RuntimeException("Unable to create test directory: " + AVRO_DIR_NAME);
      }

      recordWriter.create(avroSchema, new File(AVRO_DIR_NAME, AVRO_NAME));
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericData.Record record = new GenericData.Record(avroSchema);

        TDigest tDigest = TDigest.createMergingDigest(PercentileTDigestValueAggregator.DEFAULT_TDIGEST_COMPRESSION);
        tDigest.add(_random.nextDouble());

        ByteBuffer buffer = ByteBuffer.allocate(tDigest.byteSize());
        tDigest.asBytes(buffer);
        _fixedExpected.add(buffer.array());

        buffer.flip();
        record.put(FIXED_BYTES_UNSORTED_COLUMN, buffer);

        if (i % 2 == 0) {
          tDigest.add(_random.nextDouble());
        }

        buffer = ByteBuffer.allocate(tDigest.byteSize());
        tDigest.asBytes(buffer);
        _varExpected.add(buffer.array());

        buffer.flip();
        record.put(VARIABLE_BYTES_COLUMN, buffer);

        recordWriter.append(record);
      }
    }
  }

  /**
   * Helper method that builds a segment from the given avro file.
   *
   * @param schema Schema of data
   * @return Pinot Segment
   */
  private IndexSegment buildSegmentFromAvro(Schema schema, String dirName, String avroName, String segmentName)
      throws Exception {
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(_tableConfig, schema);
    config.setInputFilePath(dirName + File.separator + avroName);
    config.setOutDir(dirName);
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();

    return ImmutableSegmentLoader.load(new File(AVRO_DIR_NAME, SEGMENT_NAME), ReadMode.mmap);
  }
}
