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
package org.apache.pinot.segments.v1.creator;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.creator.StatsCollectorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import org.apache.pinot.core.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import org.apache.pinot.core.segment.creator.impl.stats.BytesColumnPredIndexStatsCollector;
import org.apache.pinot.core.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import org.apache.pinot.core.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import org.apache.pinot.core.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import org.apache.pinot.core.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import org.apache.pinot.core.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.core.segment.index.readers.DoubleDictionary;
import org.apache.pinot.core.segment.index.readers.FloatDictionary;
import org.apache.pinot.core.segment.index.readers.IntDictionary;
import org.apache.pinot.core.segment.index.readers.LongDictionary;
import org.apache.pinot.core.segment.index.readers.StringDictionary;
import org.apache.pinot.util.TestUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DictionariesTest {
  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static File INDEX_DIR = new File(DictionariesTest.class.toString());
  static Map<String, Set<Object>> uniqueEntries;

  private static File segmentDirectory;

  private static TableConfig _tableConfig ;

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

    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day", TimeUnit.DAYS,
            "test");
    _tableConfig = config.getTableConfig();

    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    config.setSkipTimeValueCheck(true);
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    final Schema schema = AvroUtils.getPinotSchemaFromAvroDataFile(new File(filePath));

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }

    uniqueEntries = new HashMap<>();
    for (final String column : columns) {
      uniqueEntries.put(column, new HashSet<>());
    }

    while (avroReader.hasNext()) {
      final GenericRecord rec = avroReader.next();
      for (final String column : columns) {
        Object val = rec.get(column);
        if (val instanceof Utf8) {
          val = ((Utf8) val).toString();
        }
        uniqueEntries.get(column).add(val);
      }
    }
  }

  @Test
  public void test1()
      throws Exception {
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(segmentDirectory, ReadMode.heap);
    ImmutableSegment mmapSegment = ImmutableSegmentLoader.load(segmentDirectory, ReadMode.mmap);

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
          break;
        case LONG:
          Assert.assertTrue(heapDictionary instanceof LongDictionary);
          Assert.assertTrue(mmapDictionary instanceof LongDictionary);
          break;
        case FLOAT:
          Assert.assertTrue(heapDictionary instanceof FloatDictionary);
          Assert.assertTrue(mmapDictionary instanceof FloatDictionary);
          break;
        case DOUBLE:
          Assert.assertTrue(heapDictionary instanceof DoubleDictionary);
          Assert.assertTrue(mmapDictionary instanceof DoubleDictionary);
          break;
        case STRING:
          Assert.assertTrue(heapDictionary instanceof StringDictionary);
          Assert.assertTrue(mmapDictionary instanceof StringDictionary);
          break;
        default:
          Assert.fail();
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
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(segmentDirectory, ReadMode.heap);
    ImmutableSegment mmapSegment = ImmutableSegmentLoader.load(segmentDirectory, ReadMode.mmap);

    Schema schema = heapSegment.getSegmentMetadata().getSchema();
    for (String columnName : schema.getPhysicalColumnNames()) {
      Dictionary heapDictionary = heapSegment.getDictionary(columnName);
      Dictionary mmapDictionary = mmapSegment.getDictionary(columnName);

      for (Object entry : uniqueEntries.get(columnName)) {
        String stringValue = entry.toString();
        Assert.assertEquals(mmapDictionary.indexOf(stringValue), heapDictionary.indexOf(stringValue));
        if (!columnName.equals("pageKey")) {
          Assert.assertFalse(heapDictionary.indexOf(stringValue) < 0);
          Assert.assertFalse(mmapDictionary.indexOf(stringValue) < 0);
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

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(inputStrings, fieldSpec, indexDir)) {
      dictionaryCreator.build();
      for (String inputString : inputStrings) {
        Assert
            .assertTrue(dictionaryCreator.indexOfSV(inputString) >= 0, "Value not found in dictionary " + inputString);
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

    try (SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(new String[]{""}, fieldSpec,
        indexDir)) {
      dictionaryCreator.build();
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

    switch (dataType) {
      case INT:
        return new IntColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case LONG:
        return new LongColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BOOLEAN:
      case STRING:
        return new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);
      case BYTES:
        return new BytesColumnPredIndexStatsCollector(column, statsCollectorConfig);
      default:
        throw new IllegalArgumentException("Illegal data type for stats builder: " + dataType);
    }
  }
}
