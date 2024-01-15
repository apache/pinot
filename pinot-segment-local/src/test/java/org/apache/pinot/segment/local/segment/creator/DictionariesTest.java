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
package org.apache.pinot.segment.local.segment.creator;

import java.io.File;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.segment.local.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BigDecimalColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.BytesColumnPredIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.local.segment.index.readers.BigDecimalDictionary;
import org.apache.pinot.segment.local.segment.index.readers.DoubleDictionary;
import org.apache.pinot.segment.local.segment.index.readers.FloatDictionary;
import org.apache.pinot.segment.local.segment.index.readers.IntDictionary;
import org.apache.pinot.segment.local.segment.index.readers.LongDictionary;
import org.apache.pinot.segment.local.segment.index.readers.StringDictionary;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DictionariesTest {
  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static final File INDEX_DIR = new File(DictionariesTest.class.toString());
  private static final Map<String, Set<Object>> UNIQUE_ENTRIES = new HashMap<>();

  private static File _segmentDirectory;

  private static TableConfig _tableConfig;

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before()
      throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(DictionariesTest.class.getClassLoader().getResource(AVRO_DATA));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            TimeUnit.DAYS, "test");
    _tableConfig = config.getTableConfig();
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    final Schema schema = AvroUtils.getPinotSchemaFromAvroDataFile(new File(filePath));

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }

    for (final String column : columns) {
      UNIQUE_ENTRIES.put(column, new HashSet<>());
    }

    while (avroReader.hasNext()) {
      final GenericRecord rec = avroReader.next();
      for (final String column : columns) {
        Object val = rec.get(column);
        if (val instanceof Utf8) {
          val = ((Utf8) val).toString();
        }
        UNIQUE_ENTRIES.get(column).add(val);
      }
    }
  }

  @Test
  public void test1()
      throws Exception {
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.heap);
    ImmutableSegment mmapSegment = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.mmap);

    Schema schema = heapSegment.getSegmentMetadata().getSchema();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      // Skip virtual columns
      if (fieldSpec.isVirtualColumn()) {
        continue;
      }

      String columnName = fieldSpec.getName();
      Dictionary heapDictionary = heapSegment.getDictionary(columnName);
      Dictionary mmapDictionary = mmapSegment.getDictionary(columnName);

      switch (fieldSpec.getDataType()) {
        case INT:
          Assert.assertTrue(heapDictionary instanceof IntDictionary);
          Assert.assertTrue(mmapDictionary instanceof IntDictionary);
          int firstInt = heapDictionary.getIntValue(0);
          Assert.assertEquals(heapDictionary.indexOf(firstInt), heapDictionary.indexOf(String.valueOf(firstInt)));
          Assert.assertEquals(mmapDictionary.indexOf(firstInt), mmapDictionary.indexOf(String.valueOf(firstInt)));
          break;
        case LONG:
          Assert.assertTrue(heapDictionary instanceof LongDictionary);
          Assert.assertTrue(mmapDictionary instanceof LongDictionary);
          long firstLong = heapDictionary.getLongValue(0);
          Assert.assertEquals(heapDictionary.indexOf(firstLong), heapDictionary.indexOf(String.valueOf(firstLong)));
          Assert.assertEquals(mmapDictionary.indexOf(firstLong), mmapDictionary.indexOf(String.valueOf(firstLong)));
          break;
        case FLOAT:
          Assert.assertTrue(heapDictionary instanceof FloatDictionary);
          Assert.assertTrue(mmapDictionary instanceof FloatDictionary);
          float firstFloat = heapDictionary.getFloatValue(0);
          Assert.assertEquals(heapDictionary.indexOf(firstFloat), heapDictionary.indexOf(String.valueOf(firstFloat)));
          Assert.assertEquals(mmapDictionary.indexOf(firstFloat), mmapDictionary.indexOf(String.valueOf(firstFloat)));
          break;
        case DOUBLE:
          Assert.assertTrue(heapDictionary instanceof DoubleDictionary);
          Assert.assertTrue(mmapDictionary instanceof DoubleDictionary);
          double firstDouble = heapDictionary.getDoubleValue(0);
          Assert.assertEquals(heapDictionary.indexOf(firstDouble), heapDictionary.indexOf(String.valueOf(firstDouble)));
          Assert.assertEquals(mmapDictionary.indexOf(firstDouble), mmapDictionary.indexOf(String.valueOf(firstDouble)));
          break;
        case BIG_DECIMAL:
          Assert.assertTrue(heapDictionary instanceof BigDecimalDictionary);
          Assert.assertTrue(mmapDictionary instanceof BigDecimalDictionary);
          break;
        case STRING:
          Assert.assertTrue(heapDictionary instanceof StringDictionary);
          Assert.assertTrue(mmapDictionary instanceof StringDictionary);
          break;
        default:
          Assert.fail();
          break;
      }

      Assert.assertEquals(mmapDictionary.length(), heapDictionary.length());
      for (int i = 0; i < heapDictionary.length(); i++) {
        Assert.assertEquals(mmapDictionary.get(i), heapDictionary.get(i));
      }
    }
  }

  @Test
  public void test2()
      throws Exception {
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.heap);
    ImmutableSegment mmapSegment = ImmutableSegmentLoader.load(_segmentDirectory, ReadMode.mmap);

    Schema schema = heapSegment.getSegmentMetadata().getSchema();
    for (String columnName : schema.getPhysicalColumnNames()) {
      Dictionary heapDictionary = heapSegment.getDictionary(columnName);
      Dictionary mmapDictionary = mmapSegment.getDictionary(columnName);

      for (Object entry : UNIQUE_ENTRIES.get(columnName)) {
        String stringValue = entry.toString();
        Assert.assertEquals(mmapDictionary.indexOf(stringValue), heapDictionary.indexOf(stringValue));
        if (!columnName.equals("pageKey")) {
          Assert.assertFalse(heapDictionary.indexOf(stringValue) < 0);
          Assert.assertFalse(mmapDictionary.indexOf(stringValue) < 0);
        }
        if (entry instanceof Integer) {
          Assert.assertEquals(mmapDictionary.indexOf((int) entry), mmapDictionary.indexOf(stringValue));
          Assert.assertEquals(heapDictionary.indexOf((int) entry), heapDictionary.indexOf(stringValue));
        } else if (entry instanceof Long) {
          Assert.assertEquals(mmapDictionary.indexOf((long) entry), mmapDictionary.indexOf(stringValue));
          Assert.assertEquals(heapDictionary.indexOf((long) entry), heapDictionary.indexOf(stringValue));
        }
      }
    }
  }

  @Test
  public void testIntColumnPreIndexStatsCollector() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.INT);
    statsCollector.collect(1);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(3);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(40);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(20);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testFloatColumnPreIndexStatsCollector() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.FLOAT);
    statsCollector.collect(1f);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2f);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(3f);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4f);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4f);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2f);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(40f);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(20f);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testLongColumnPreIndexStatsCollector() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.LONG);
    statsCollector.collect(1L);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2L);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(3L);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4L);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4L);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2L);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(40L);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(20L);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testDoubleColumnPreIndexStatsCollector() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.DOUBLE);
    statsCollector.collect(1d);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2d);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(3d);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4d);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(4d);
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(2d);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(40d);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(20d);
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testBigDecimalColumnPreIndexStatsCollector() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.BIG_DECIMAL, false);
    statsCollector.collect(BigDecimal.valueOf(1d));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(2d));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(3d));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(4d));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(4d));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(2d));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(40d));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(BigDecimal.valueOf(20d));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testStringColumnPreIndexStatsCollectorForRandomString() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.STRING);
    statsCollector.collect("a");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("b");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("c");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("d");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("d");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("b");
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect("z");
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect("u");
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals((statsCollector.getMinValue()).toString(), "a");
    Assert.assertEquals((statsCollector.getMaxValue()).toString(), "z");
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testStringColumnPreIndexStatsCollectorForBoolean() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.BOOLEAN);
    statsCollector.collect("false");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("false");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("false");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("true");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("true");
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect("false");
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect("false");
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect("true");
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 2);
    Assert.assertEquals((statsCollector.getMinValue()).toString(), "false");
    Assert.assertEquals((statsCollector.getMaxValue()).toString(), "true");
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testBytesColumnPreIndexStatsCollector() {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.BYTES);
    statsCollector.collect(new byte[]{1});
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new byte[]{1});
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new byte[]{1, 2});
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new byte[]{1, 2, 3});
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new byte[]{1, 2, 3, 4});
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new byte[]{0});
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new byte[]{0, 1});
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new byte[]{1});
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(statsCollector.getMinValue(), new ByteArray(new byte[]{0}));
    Assert.assertEquals(statsCollector.getMaxValue(), new ByteArray(new byte[]{1, 2, 3, 4}));
    Assert.assertFalse(statsCollector.isSorted());
  }

  /**
   * Test for ensuring that Strings with special characters can be handled
   * correctly.
   *
   * @throws Exception
   */
  @Test
  public void testUTF8Characters()
      throws Exception {
    File indexDir = new File("/tmp/dict.test");
    indexDir.deleteOnExit();
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true);

    String[] inputStrings = new String[3];
    inputStrings[0] = new String(new byte[]{67, 97, 102, -61, -87}); // "Café";
    inputStrings[1] = new String(new byte[]{70, 114, 97, 110, -61, -89, 111, 105, 115}); // "François";
    inputStrings[2] =
        new String(new byte[]{67, -61, -76, 116, 101, 32, 100, 39, 73, 118, 111, 105, 114, 101}); // "Côte d'Ivoire";
    Arrays.sort(inputStrings);

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(fieldSpec, indexDir)) {
      dictionaryCreator.build(inputStrings);
      for (String inputString : inputStrings) {
        Assert.assertTrue(dictionaryCreator.indexOfSV(inputString) >= 0,
            "Value not found in dictionary " + inputString);
      }
    }

    FileUtils.deleteQuietly(indexDir);
  }

  /**
   * Tests SegmentDictionaryCreator for case when there is only one string and it is empty.
   */
  @Test
  public void testSingleEmptyString()
      throws Exception {
    File indexDir = new File("/tmp/dict.test");
    indexDir.deleteOnExit();
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true);

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(fieldSpec, indexDir)) {
      dictionaryCreator.build(new String[]{""});
      Assert.assertEquals(dictionaryCreator.getNumBytesPerEntry(), 0);
      Assert.assertEquals(dictionaryCreator.indexOfSV(""), 0);
    }

    FileUtils.deleteQuietly(indexDir);
  }

  /**
   * Helper method to build stats collector for a given column.
   *
   * @param column Column name
   * @param dataType Data type for the column
   * @return StatsCollector for the column
   */
  private AbstractColumnStatisticsCollector buildStatsCollector(String column, DataType dataType) {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(column, dataType, true));
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(_tableConfig, schema, null);
    return buildStatsCollector(column, dataType, statsCollectorConfig);
  }

  private AbstractColumnStatisticsCollector buildStatsCollector(String column, DataType dataType,
      boolean isDimensionField) {
    if (isDimensionField) {
      return buildStatsCollector(column, dataType);
    }

    Schema schema = new Schema();
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec(column, dataType);
    metricFieldSpec.setSingleValueField(true);
    schema.addField(metricFieldSpec);
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(_tableConfig, schema, null);
    return buildStatsCollector(column, dataType, statsCollectorConfig);
  }

  private AbstractColumnStatisticsCollector buildStatsCollector(String column, DataType dataType,
      StatsCollectorConfig statsCollectorConfig) {
    switch (dataType) {
      case INT:
        return new IntColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case LONG:
        return new LongColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BIG_DECIMAL:
        return new BigDecimalColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BOOLEAN:
      case STRING:
        return new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BYTES:
        return new BytesColumnPredIndexStatsCollector(column, statsCollectorConfig);
      default:
        throw new IllegalArgumentException("Illegal data type for stats builder: " + dataType);
    }
  }

  @Test
  public void clpTest() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("column1", DataType.STRING, true));
    List<FieldConfig> fieldConfigList = new ArrayList<>();
    fieldConfigList.add(new FieldConfig("column1", FieldConfig.EncodingType.RAW, Collections.EMPTY_LIST,
        FieldConfig.CompressionCodec.CLP, Collections.EMPTY_MAP));
    _tableConfig.setFieldConfigList(fieldConfigList);
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(_tableConfig, schema, null);
    StringColumnPreIndexStatsCollector statsCollector =
        new StringColumnPreIndexStatsCollector("column1", statsCollectorConfig);
    statsCollector.collect(
        "2023/10/26 00:03:10.168 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32c_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32c_DEFAULT : Refreshed 35 property LiveInstance took 5 ms. Selective: true");
    statsCollector.collect(
        "2023/10/26 00:03:10.169 INFO [PropertyCache] [HelixController-pipeline-default-pinot-(4a02a32d_DEFAULT)] "
            + "Event pinot::DEFAULT::4a02a32d_DEFAULT : Refreshed 81 property LiveInstance took 4 ms. Selective: true");

    statsCollector.seal();
    System.out.println(statsCollector.getClpStats());
  }
}
