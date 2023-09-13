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

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.io.FileHandler;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class SegmentColumnarIndexCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentColumnarIndexCreatorTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String COLUMN_NAME = "testColumn";
  private static final String COLUMN_PROPERTY_KEY_PREFIX = Column.COLUMN_PROPS_KEY_PREFIX + COLUMN_NAME + ".";

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testRemoveColumnMetadataInfo()
      throws Exception {
    PropertiesConfiguration configuration = new PropertiesConfiguration();
    FileHandler fileHandler = new FileHandler(configuration);
    fileHandler.load(CONFIG_FILE);
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "a", "foo");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "b", "bar");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "c", "foobar");
    fileHandler.save();

    configuration = new PropertiesConfiguration();
    fileHandler = new FileHandler(configuration);
    fileHandler.load(CONFIG_FILE);
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(configuration, COLUMN_NAME);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    fileHandler.save();

    configuration = new PropertiesConfiguration();
    fileHandler = new FileHandler(configuration);
    fileHandler.load(CONFIG_FILE);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
  }

  @Test
  public void testTimeColumnInMetadata()
      throws Exception {
    long withTZ =
        getStartTimeInSegmentMetadata("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ssZ", "2021-07-21T06:48:51Z");
    long withoutTZ =
        getStartTimeInSegmentMetadata("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss", "2021-07-21T06:48:51");
    assertEquals(withTZ, 1626850131000L); // as UTC timestamp.
    assertEquals(withTZ, withoutTZ);
  }

  private static long getStartTimeInSegmentMetadata(String testDateTimeFormat, String testDateTime)
      throws Exception {
    String timeColumn = "foo";
    Schema schema =
        new Schema.SchemaBuilder().addDateTime(timeColumn, DataType.STRING, testDateTimeFormat, "1:MILLISECONDS")
            .build();
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(timeColumn).build();

    String segmentName = "testSegment";
    String indexDirPath = new File(TEMP_DIR, segmentName).getAbsolutePath();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(indexDirPath);
    config.setSegmentName(segmentName);
    try {
      FileUtils.deleteQuietly(new File(indexDirPath));

      GenericRow row = new GenericRow();
      row.putValue(timeColumn, testDateTime);
      List<GenericRow> rows = ImmutableList.of(row);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(rows));
      driver.build();
      IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(indexDirPath, segmentName), ReadMode.heap);
      SegmentMetadata md = indexSegment.getSegmentMetadata();
      return md.getStartTime();
    } finally {
      FileUtils.deleteQuietly(new File(indexDirPath));
    }
  }

  @Test
  public void testGetValueWithinLengthLimit() {
    // String value without '\uFFFF' suffix
    String value = RandomStringUtils.randomAscii(SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT + 1);
    String minValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, false, DataType.STRING);
    assertEquals(minValue, value.substring(0, SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT));
    assertTrue(minValue.compareTo(value) < 0);
    String maxValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, true, DataType.STRING);
    assertEquals(maxValue,
        value.substring(0, SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT - 1) + '\uFFFF');
    assertTrue(maxValue.compareTo(value) > 0);

    // String value with '\uFFFF' suffix
    value =
        RandomStringUtils.randomAscii(SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT - 1) + "\uFFFF\uFFFF";
    minValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, false, DataType.STRING);
    assertEquals(minValue, value.substring(0, SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT));
    assertTrue(minValue.compareTo(value) < 0);
    maxValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, true, DataType.STRING);
    assertEquals(maxValue, value);

    // String value with '\uFFFF' and another character
    value = RandomStringUtils.randomAscii(SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT - 1) + "\uFFFFa";
    minValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, false, DataType.STRING);
    assertEquals(minValue, value.substring(0, SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT));
    assertTrue(minValue.compareTo(value) < 0);
    maxValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, true, DataType.STRING);
    assertEquals(maxValue, value.substring(0, SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT) + '\uFFFF');
    assertTrue(maxValue.compareTo(value) > 0);

    // Bytes value without 0xFF suffix
    int numBytes = SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT / 2 + 1;
    byte[] bytes = new byte[numBytes];
    Random random = new Random();
    random.nextBytes(bytes);
    bytes[numBytes - 2] = 5;
    value = BytesUtils.toHexString(bytes);
    minValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, false, DataType.BYTES);
    byte[] minBytes = BytesUtils.toBytes(minValue);
    assertEquals(minBytes.length, numBytes - 1);
    assertEquals(Arrays.copyOfRange(minBytes, 0, numBytes - 1), Arrays.copyOfRange(bytes, 0, numBytes - 1));
    assertTrue(ByteArray.compare(minBytes, bytes) < 0);
    maxValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, true, DataType.BYTES);
    byte[] maxBytes = BytesUtils.toBytes(maxValue);
    assertEquals(maxBytes.length, numBytes - 1);
    assertEquals(Arrays.copyOfRange(maxBytes, 0, numBytes - 2), Arrays.copyOfRange(bytes, 0, numBytes - 2));
    assertEquals(maxBytes[numBytes - 2], (byte) 0xFF);
    assertTrue(ByteArray.compare(maxBytes, bytes) > 0);

    // Bytes value with 0xFF suffix
    bytes[numBytes - 2] = (byte) 0xFF;
    bytes[numBytes - 1] = (byte) 0xFF;
    value = BytesUtils.toHexString(bytes);
    minValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, false, DataType.BYTES);
    minBytes = BytesUtils.toBytes(minValue);
    assertEquals(minBytes.length, numBytes - 1);
    assertEquals(Arrays.copyOfRange(minBytes, 0, numBytes - 1), Arrays.copyOfRange(bytes, 0, numBytes - 1));
    assertTrue(ByteArray.compare(minBytes, bytes) < 0);
    maxValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, true, DataType.BYTES);
    assertEquals(maxValue, value);

    // Bytes value with 0xFF and another byte
    bytes[numBytes - 1] = 5;
    value = BytesUtils.toHexString(bytes);
    minValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, false, DataType.BYTES);
    minBytes = BytesUtils.toBytes(minValue);
    assertEquals(minBytes.length, numBytes - 1);
    assertEquals(Arrays.copyOfRange(minBytes, 0, numBytes - 1), Arrays.copyOfRange(bytes, 0, numBytes - 1));
    assertTrue(ByteArray.compare(minBytes, bytes) < 0);
    maxValue = SegmentColumnarIndexCreator.getValueWithinLengthLimit(value, true, DataType.BYTES);
    maxBytes = BytesUtils.toBytes(maxValue);
    assertEquals(maxBytes.length, numBytes);
    assertEquals(Arrays.copyOfRange(maxBytes, 0, numBytes - 1), Arrays.copyOfRange(bytes, 0, numBytes - 1));
    assertEquals(maxBytes[numBytes - 1], (byte) 0xFF);
    assertTrue(ByteArray.compare(maxBytes, bytes) > 0);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
