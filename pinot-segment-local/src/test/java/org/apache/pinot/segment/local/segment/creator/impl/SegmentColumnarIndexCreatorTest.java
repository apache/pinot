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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.PinotBuffersAfterClassCheckRule;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentColumnReaderFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.V1Constants.MetadataKeys.Column;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class SegmentColumnarIndexCreatorTest implements PinotBuffersAfterClassCheckRule {
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
    PropertiesConfiguration configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE);
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "a", "foo");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "b", "bar");
    configuration.setProperty(COLUMN_PROPERTY_KEY_PREFIX + "c", "foobar");
    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE);

    configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE);
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertTrue(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    SegmentColumnarIndexCreator.removeColumnMetadataInfo(configuration, COLUMN_NAME);
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "a"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "b"));
    assertFalse(configuration.containsKey(COLUMN_PROPERTY_KEY_PREFIX + "c"));
    CommonsConfigurationUtils.saveToFile(configuration, CONFIG_FILE);

    configuration = CommonsConfigurationUtils.fromFile(CONFIG_FILE);
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

  @DataProvider
  public Object[][] forwardIndexEncodings() {
    return new Object[][]{{EncodingType.DICTIONARY}, {EncodingType.RAW}};
  }

  @Test(dataProvider = "forwardIndexEncodings")
  public void testExplicitMultiValueLimit(EncodingType encoding)
      throws Exception {
    int limit = 4;
    Schema schema = new Schema.SchemaBuilder().addMultiValueDimension(COLUMN_NAME, DataType.LONG).build();
    FieldConfig fieldConfig = new FieldConfig.Builder(COLUMN_NAME).withEncodingType(encoding).build();
    FieldConfig limitedFieldConfig =
        new FieldConfig.Builder(fieldConfig).withProperties(Map.of("maxNumMultiValues", String.valueOf(limit))).build();
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE).setTableName("test");
    TableConfig limitedTableConfig = tableConfigBuilder.setFieldConfigList(List.of(limitedFieldConfig)).build();
    TableConfig unlimitedTableConfig = tableConfigBuilder.setFieldConfigList(List.of(fieldConfig)).build();

    SegmentGeneratorConfig limitedConfig = new SegmentGeneratorConfig(limitedTableConfig, schema);
    limitedConfig.setOutDir(new File(TEMP_DIR, encoding.name()).getAbsolutePath());
    limitedConfig.setSegmentName("atLimit");
    buildMultiValueSegment(limitedConfig, limit);
    ImmutableSegment segment =
        ImmutableSegmentLoader.load(new File(limitedConfig.getOutDir(), "atLimit"), ReadMode.heap);
    try {
      assertEquals(segment.getSegmentMetadata().getColumnMetadataFor(COLUMN_NAME).getMaxNumberOfMultiValues(), limit);
      assertEquals(segment.getDataSource(COLUMN_NAME).getForwardIndex().isDictionaryEncoded(),
          encoding == EncodingType.DICTIONARY);
    } finally {
      segment.destroy();
    }

    limitedConfig.setSegmentName("overLimit");
    String error = "Number of values: 5 in MV column: " + COLUMN_NAME + " exceeds the maximum allowed: " + limit;
    assertEquals(expectThrows(IllegalStateException.class, () -> buildMultiValueSegment(limitedConfig, limit + 1))
        .getMessage(), error);

    SegmentGeneratorConfig unlimitedConfig = new SegmentGeneratorConfig(unlimitedTableConfig, schema);
    unlimitedConfig.setOutDir(limitedConfig.getOutDir());
    unlimitedConfig.setSegmentName("unlimited");
    buildMultiValueSegment(unlimitedConfig, 1001);
    ImmutableSegment sourceSegment =
        ImmutableSegmentLoader.load(new File(unlimitedConfig.getOutDir(), "unlimited"), ReadMode.heap);
    try (PinotSegmentColumnReaderFactory factory = new PinotSegmentColumnReaderFactory(sourceSegment)) {
      assertEquals(sourceSegment.getSegmentMetadata().getColumnMetadataFor(COLUMN_NAME).getMaxNumberOfMultiValues(),
          1001);
      limitedConfig.setSegmentName("overLimitColumnar");
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(limitedConfig, factory);
      assertEquals(expectThrows(IllegalStateException.class, driver::build).getMessage(),
          "Number of values: 1001 in MV column: " + COLUMN_NAME + " exceeds the maximum allowed: " + limit);
    } finally {
      sourceSegment.destroy();
    }
  }

  @Test
  public void testExplicitMultiValueLimitWithDisabledForwardIndex()
      throws Exception {
    Schema schema = new Schema.SchemaBuilder().addMultiValueDimension(COLUMN_NAME, DataType.LONG).build();
    FieldConfig fieldConfig = new FieldConfig.Builder(COLUMN_NAME)
        .withProperties(Map.of(FieldConfig.FORWARD_INDEX_DISABLED, "true", "maxNumMultiValues", "4"))
        .build();
    TableConfigBuilder tableConfigBuilder = new TableConfigBuilder(TableType.OFFLINE).setTableName("test");
    TableConfig tableConfig = tableConfigBuilder.setFieldConfigList(List.of(fieldConfig)).build();
    String outDir = new File(TEMP_DIR, "disabledForwardIndex").getAbsolutePath();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(outDir);
    config.setSegmentName("overLegacyLimit");
    assertEquals(expectThrows(IllegalStateException.class, () -> buildMultiValueSegment(config, 5)).getMessage(),
        "Number of values: 5 in MV column: " + COLUMN_NAME + " exceeds the maximum allowed: 4");

    FieldConfig modernFieldConfig = new FieldConfig.Builder(fieldConfig)
        .withIndexes(JsonUtils.stringToJsonNode("{\"forward\":{\"maxNumMultiValues\":5}}"))
        .build();
    TableConfig modernTableConfig = tableConfigBuilder.setFieldConfigList(List.of(modernFieldConfig)).build();
    SegmentGeneratorConfig modernConfig = new SegmentGeneratorConfig(modernTableConfig, schema);
    modernConfig.setOutDir(outDir);
    modernConfig.setSegmentName("overModernLimit");
    assertEquals(expectThrows(IllegalStateException.class, () -> buildMultiValueSegment(modernConfig, 6)).getMessage(),
        "Number of values: 6 in MV column: " + COLUMN_NAME + " exceeds the maximum allowed: 5");
  }

  private static void buildMultiValueSegment(SegmentGeneratorConfig config, int numValues)
      throws Exception {
    Object[] values = IntStream.range(0, numValues).asLongStream().boxed().toArray();
    GenericRow row = new GenericRow();
    row.putValue(COLUMN_NAME, values);
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(List.of(row)));
    driver.build();
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
    IndexSegment indexSegment = null;

    try {
      FileUtils.deleteQuietly(new File(indexDirPath));

      GenericRow row = new GenericRow();
      row.putValue(timeColumn, testDateTime);
      List<GenericRow> rows = List.of(row);

      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(rows));
      driver.build();
      indexSegment = ImmutableSegmentLoader.load(new File(indexDirPath, segmentName), ReadMode.heap);
      SegmentMetadata md = indexSegment.getSegmentMetadata();
      return md.getStartTime();
    } finally {
      if (indexSegment != null) {
        indexSegment.destroy();
      }
      FileUtils.deleteQuietly(new File(indexDirPath));
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(TEMP_DIR);
  }
}
