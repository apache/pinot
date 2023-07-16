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
import java.util.List;
import java.util.Random;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.MAX_VALUE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.MIN_VALUE;
import static org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column.getKeyFor;


public class SegmentColumnarIndexCreatorTest {
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentColumnarIndexCreatorTest");
  private static final File CONFIG_FILE = new File(TEMP_DIR, "config");
  private static final String PROPERTY_KEY = "testKey";
  private static final String COLUMN_NAME = "testColumn";
  private static final String COLUMN_PROPERTY_KEY_PREFIX = Column.COLUMN_PROPS_KEY_PREFIX + COLUMN_NAME + ".";
  private static final int NUM_ROUNDS = 1000;

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }

  @Test
  public void testGetValidPropertyValue()
      throws Exception {
    // Leading/trailing whitespace
    Assert.assertEquals(getEscapedValidPropertyValue(" a"), " a");
    Assert.assertEquals(getEscapedValidPropertyValue("a\t"), "a\t");
    Assert.assertEquals(getEscapedValidPropertyValue("\na"), "\na");

    // Whitespace in the middle
    Assert.assertEquals(getEscapedValidPropertyValue("a\t b"), "a\t b");
    Assert.assertEquals(getEscapedValidPropertyValue("a \nb"), "a \nb");

    // List separator
    Assert.assertEquals(getEscapedValidPropertyValue("a,b,c"), "a,b,c");
    Assert.assertEquals(getEscapedValidPropertyValue(",a b"), ",a b");

    // Empty string
    Assert.assertEquals(getEscapedValidPropertyValue(""), "");

    // Escape character for variable substitution
    Assert.assertEquals(getEscapedValidPropertyValue("$${"), "$${");
  }

  @Test
  public void testPropertyValueWithSpecialCharacters()
      throws Exception {
    // Leading/trailing whitespace
    Assert.assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue(" a"));
    Assert.assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue("a\t"));
    Assert.assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue("\na"));

    // Whitespace in the middle
    Assert.assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("a\t b"));
    testPropertyValueWithSpecialCharacters("a\t b");
    Assert.assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("a \nb"));
    testPropertyValueWithSpecialCharacters("a \nb");

    // List separator
    Assert.assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue("a,b,c"));
    Assert.assertFalse(SegmentColumnarIndexCreator.isValidPropertyValue(",a b"));

    // Empty string
    Assert.assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue(""));
    testPropertyValueWithSpecialCharacters("");

    // Variable substitution (should be disabled)
    Assert.assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("$${testKey}"));
    testPropertyValueWithSpecialCharacters("$${testKey}");

    // Escape character for variable substitution
    Assert.assertTrue(SegmentColumnarIndexCreator.isValidPropertyValue("$${"));
    testPropertyValueWithSpecialCharacters("$${");

    for (int i = 0; i < NUM_ROUNDS; i++) {
      testPropertyValueWithSpecialCharacters(RandomStringUtils.randomAscii(5));
      testPropertyValueWithSpecialCharacters(RandomStringUtils.random(5));
    }
  }

  private void testPropertyValueWithSpecialCharacters(String value)
      throws Exception {
    if (SegmentColumnarIndexCreator.isValidPropertyValue(value)) {
      PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_FILE);
      configuration.setProperty(PROPERTY_KEY, value);
      Assert.assertEquals(configuration.getProperty(PROPERTY_KEY), value);
      configuration.save();

      configuration = new PropertiesConfiguration(CONFIG_FILE);
      Assert.assertEquals(configuration.getProperty(PROPERTY_KEY), value);
    }
  }

  @Test
  public void testRemoveColumnMetadataInfo()
      throws Exception {
    PropertiesConfiguration configuration = new PropertiesConfiguration(CONFIG_FILE);
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "a", "foo");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "b", "bar");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "c", "foobar");
    configuration.save();

    configuration = new PropertiesConfiguration(CONFIG_FILE);
    Assert.assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    Assert.assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    Assert.assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(configuration, COLUMN_NAME);
    Assert.assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    Assert.assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    Assert.assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    configuration.save();

    configuration = new PropertiesConfiguration(CONFIG_FILE);
    Assert.assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    Assert.assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    Assert.assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
  }

  @Test
  public void testTimeColumnInMetadata()
      throws Exception {
    long withTZ =
        getStartTimeInSegmentMetadata("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ssZ", "2021-07-21T06:48:51Z");
    long withoutTZ =
        getStartTimeInSegmentMetadata("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd'T'HH:mm:ss", "2021-07-21T06:48:51");
    Assert.assertEquals(withTZ, 1626850131000L); // as UTC timestamp.
    Assert.assertEquals(withTZ, withoutTZ);
  }

  private static long getStartTimeInSegmentMetadata(String testDateTimeFormat, String testDateTime)
      throws Exception {
    String timeColumn = "foo";
    Schema schema = new Schema.SchemaBuilder().addDateTime(timeColumn, FieldSpec.DataType.STRING, testDateTimeFormat,
        "1:MILLISECONDS").build();
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
  public void testAddMinMaxValue() {
    PropertiesConfiguration props = new PropertiesConfiguration();
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", "bar", "foo", FieldSpec.DataType.STRING);
    Assert.assertFalse(Boolean.parseBoolean(
        String.valueOf(props.getProperty(getKeyFor("colA", Column.MIN_MAX_VALUE_INVALID)))));

    props = new PropertiesConfiguration();
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", ",bar", "foo", FieldSpec.DataType.STRING);
    Assert.assertFalse(Boolean.parseBoolean(
        String.valueOf(props.getProperty(getKeyFor("colA", Column.MIN_MAX_VALUE_INVALID)))));
    Assert.assertEquals(String.valueOf(props.getProperty(getKeyFor("colA", MIN_VALUE))), ",bar");

    props = new PropertiesConfiguration();
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", "bar", "  ", FieldSpec.DataType.STRING);
    Assert.assertFalse(Boolean.parseBoolean(
        String.valueOf(props.getProperty(getKeyFor("colA", Column.MIN_MAX_VALUE_INVALID)))));

    // test for values with leading or ending whitespace.
    props = new PropertiesConfiguration();
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", "a\t", "\nb", FieldSpec.DataType.STRING);
    Assert.assertFalse(Boolean.parseBoolean(
        String.valueOf(props.getProperty(getKeyFor("colA", Column.MIN_MAX_VALUE_INVALID)))));
    Assert.assertEquals(
        StringEscapeUtils.unescapeJava(String.valueOf(props.getProperty(getKeyFor("colA", MIN_VALUE)))), "a\t");
    Assert.assertEquals(
        StringEscapeUtils.unescapeJava(String.valueOf(props.getProperty(getKeyFor("colA", MAX_VALUE)))), "\nb");

    // test for values with 'v'.
    props = new PropertiesConfiguration();
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", "aa,bb", "aa,bb,", FieldSpec.DataType.STRING);
    Assert.assertFalse(Boolean.parseBoolean(
        String.valueOf(props.getProperty(getKeyFor("colA", Column.MIN_MAX_VALUE_INVALID)))));
    Assert.assertEquals(String.valueOf(props.getProperty(getKeyFor("colA", MIN_VALUE))), "aa,bb");
    Assert.assertEquals(String.valueOf(props.getProperty(getKeyFor("colA", MAX_VALUE))), "aa,bb,");

    // test for value length grater than METADATA_PROPERTY_LENGTH_LIMIT
    props = new PropertiesConfiguration();
    String stringMinValue = RandomStringUtils.
        randomAlphabetic(SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT + 3);
    String stringMaxValue = RandomStringUtils.
        randomAlphabetic(SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT + 3);
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", stringMinValue,
        stringMaxValue, FieldSpec.DataType.STRING);
    compareLongValuesWithColumnMinMax(stringMinValue, stringMaxValue, props, FieldSpec.DataType.STRING);

    // long value test
    props = new PropertiesConfiguration();
    String longMinValue = String.valueOf(Long.MIN_VALUE);
    String longMaxValue = String.valueOf(Long.MAX_VALUE);
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", longMinValue,
        longMaxValue, FieldSpec.DataType.LONG);
    compareLongValuesWithColumnMinMax(longMinValue, longMaxValue, props, FieldSpec.DataType.LONG);

    // int value test
    props = new PropertiesConfiguration();
    String intMinValue = String.valueOf(Integer.MIN_VALUE);
    String intMaxValue = String.valueOf(Integer.MAX_VALUE);
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", intMinValue,
        intMaxValue, FieldSpec.DataType.INT);
    compareLongValuesWithColumnMinMax(intMinValue, intMaxValue, props, FieldSpec.DataType.INT);

    // float value test
    props = new PropertiesConfiguration();
    String floatMinValue = String.valueOf(Float.MIN_VALUE);
    String floatMaxValue = String.valueOf(Float.MAX_VALUE);
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", floatMinValue,
        floatMaxValue, FieldSpec.DataType.FLOAT);
    compareLongValuesWithColumnMinMax(floatMinValue, floatMaxValue, props, FieldSpec.DataType.FLOAT);

    // Double value test
    props = new PropertiesConfiguration();
    String doubleMinValue = String.valueOf(Double.MIN_VALUE);
    String doubleMaxValue = String.valueOf(Double.MAX_VALUE);
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", doubleMinValue,
        doubleMaxValue, FieldSpec.DataType.DOUBLE);
    compareLongValuesWithColumnMinMax(doubleMinValue, doubleMaxValue, props, FieldSpec.DataType.DOUBLE);

    // Byte value test
    props = new PropertiesConfiguration();
    Random random = new Random();
    byte[] byteMinValue = new byte[SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT + 3];
    byte[] byteMaxValue = new byte[SegmentColumnarIndexCreator.METADATA_PROPERTY_LENGTH_LIMIT + 3];
    random.nextBytes(byteMinValue);
    random.nextBytes(byteMaxValue);
    String byteMinValueString = BytesUtils.toHexString(byteMinValue);
    String byteMaxValueString = BytesUtils.toHexString(byteMaxValue);
    SegmentColumnarIndexCreator.addColumnMinMaxValueInfo(props, "colA", byteMinValueString,
        byteMaxValueString, FieldSpec.DataType.BYTES);
    compareLongValuesWithColumnMinMax(byteMinValueString, byteMaxValueString, props, FieldSpec.DataType.BYTES);
  }

  private void compareLongValuesWithColumnMinMax(String longMinValue, String longMaxValue,
      PropertiesConfiguration props, FieldSpec.DataType dataType) {
    Assert.assertFalse(Boolean.parseBoolean(
        String.valueOf(props.getProperty(getKeyFor("colA", Column.MIN_MAX_VALUE_INVALID)))));

    String columnMinValue = getEscapedValidPropertyValue((String) props.getProperty(getKeyFor("colA", MIN_VALUE)));
    String columnMaxValue = getEscapedValidPropertyValue((String) props.getProperty(getKeyFor("colA", MAX_VALUE)));
    Assert.assertEquals(columnMinValue,
          SegmentColumnarIndexCreator.getValidPropertyValue(longMinValue, false, dataType));
    Assert.assertEquals(columnMaxValue,
          SegmentColumnarIndexCreator.getValidPropertyValue(longMaxValue, true, dataType));


    Assert.assertTrue(columnMinValue.compareTo(longMinValue) <= 0);
    Assert.assertTrue(columnMaxValue.compareTo(longMaxValue) >= 0);
  }

  private String getEscapedValidPropertyValue(String input) {
    return StringEscapeUtils.unescapeJava(input);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
