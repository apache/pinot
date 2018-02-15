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
package com.linkedin.pinot.segments.v1.creator;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.StatsCollectorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.creator.impl.stats.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.DoubleColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.FloatColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.IntColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.LongColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.creator.impl.stats.StringColumnPreIndexStatsCollector;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
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
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DictionariesTest {
  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static File INDEX_DIR = new File(DictionariesTest.class.toString());
  static Map<String, Set<Object>> uniqueEntries;

  private static File segmentDirectory;

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

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    final Schema schema = AvroUtils.extractSchemaFromAvro(new File(filePath));

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }

    uniqueEntries = new HashMap<String, Set<Object>>();
    for (final String column : columns) {
      uniqueEntries.put(column, new HashSet<Object>());
    }

    while (avroReader.hasNext()) {
      final GenericRecord rec = avroReader.next();
      for (final String column : columns) {
        Object val = rec.get(column);
        if (val instanceof Utf8) {
          val = ((Utf8) val).toString();
        }
        uniqueEntries.get(column).add(getAppropriateType(schema.getFieldSpecFor(column).getDataType(), val));
      }
    }
  }

  private static Object getAppropriateType(DataType spec, Object val) {
    if (val == null) {
      switch (spec) {
        case DOUBLE:
          return V1Constants.Numbers.NULL_DOUBLE;
        case FLOAT:
          return V1Constants.Numbers.NULL_FLOAT;
        case INT:
          return V1Constants.Numbers.NULL_INT;
        case LONG:
          return V1Constants.Numbers.NULL_LONG;
        default:
          return V1Constants.Str.NULL_STRING;
      }
    }
    return val;
  }

  @Test
  public void test1()
      throws Exception {
    final IndexSegmentImpl heapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(segmentDirectory, ReadMode.heap);
    final IndexSegmentImpl mmapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(segmentDirectory, ReadMode.mmap);

    for (final String column : ((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap()
        .keySet()) {
      final ImmutableDictionaryReader heapDictionary = heapSegment.getDictionaryFor(column);
      final ImmutableDictionaryReader mmapDictionary = mmapSegment.getDictionaryFor(column);

      switch (((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap()
          .get(column)
          .getDataType()) {
        case BOOLEAN:
        case STRING:
          Assert.assertTrue(heapDictionary instanceof StringDictionary);
          Assert.assertTrue(mmapDictionary instanceof StringDictionary);
          break;
        case DOUBLE:
          Assert.assertTrue(heapDictionary instanceof DoubleDictionary);
          Assert.assertTrue(mmapDictionary instanceof DoubleDictionary);
          break;
        case FLOAT:
          Assert.assertTrue(heapDictionary instanceof FloatDictionary);
          Assert.assertTrue(mmapDictionary instanceof FloatDictionary);
          break;
        case LONG:
          Assert.assertTrue(heapDictionary instanceof LongDictionary);
          Assert.assertTrue(mmapDictionary instanceof LongDictionary);
          break;
        case INT:
          Assert.assertTrue(heapDictionary instanceof IntDictionary);
          Assert.assertTrue(mmapDictionary instanceof IntDictionary);
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
    final IndexSegmentImpl heapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(segmentDirectory, ReadMode.heap);
    final IndexSegmentImpl mmapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(segmentDirectory, ReadMode.mmap);

    final Map<String, ColumnMetadata> metadataMap =
        ((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap();
    for (final String column : metadataMap.keySet()) {
      final ImmutableDictionaryReader heapDictionary = heapSegment.getDictionaryFor(column);
      final ImmutableDictionaryReader mmapDictionary = mmapSegment.getDictionaryFor(column);

      final Set<Object> uniques = uniqueEntries.get(column);
      final List<Object> list = Arrays.asList(uniques.toArray());
      Collections.shuffle(list);
      for (final Object entry : list) {
        Assert.assertEquals(mmapDictionary.indexOf(entry), heapDictionary.indexOf(entry));
        if (!column.equals("pageKey")) {
          Assert.assertFalse(heapDictionary.indexOf(entry) < 0);
          Assert.assertFalse(mmapDictionary.indexOf(entry) < 0);
        }
      }
    }
  }

  @Test
  public void testIntColumnPreIndexStatsCollector()
      throws Exception {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.INT);
    statsCollector.collect(new Integer(1));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Long(3));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Double(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Integer(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(40));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(20));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testFloatColumnPreIndexStatsCollector()
      throws Exception {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.FLOAT);
    statsCollector.collect(new Integer(1));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Long(3));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Double(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Integer(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(40));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(20));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testLongColumnPreIndexStatsCollector()
      throws Exception {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.LONG);
    statsCollector.collect(new Integer(1));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Long(3));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Double(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Integer(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(40));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(20));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testDoubleColumnPreIndexStatsCollector()
      throws Exception {
    AbstractColumnStatisticsCollector statsCollector = buildStatsCollector("column1", DataType.DOUBLE);
    statsCollector.collect(new Integer(1));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Long(3));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Double(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Integer(4));
    Assert.assertTrue(statsCollector.isSorted());
    statsCollector.collect(new Float(2));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(40));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.collect(new Double(20));
    Assert.assertFalse(statsCollector.isSorted());
    statsCollector.seal();
    Assert.assertEquals(statsCollector.getCardinality(), 6);
    Assert.assertEquals(((Number) statsCollector.getMinValue()).intValue(), 1);
    Assert.assertEquals(((Number) statsCollector.getMaxValue()).intValue(), 40);
    Assert.assertFalse(statsCollector.isSorted());
  }

  @Test
  public void testStringColumnPreIndexStatsCollectorForRandomString()
      throws Exception {
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
  public void testStringColumnPreIndexStatsCollectorForBoolean()
      throws Exception {
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

  /**
   * Test for ensuring that Strings with special characters can be handled
   * correctly.
   *
   * @throws Exception
   */
  @Test
  public void testUTF8Characters() throws Exception {
    File indexDir = new File("/tmp/dict.test");
    indexDir.deleteOnExit();
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true);

    String[] inputStrings = new String[3];
    inputStrings[0] = new String(new byte[]{67, 97, 102, -61, -87}); // "Café";
    inputStrings[1] = new String(new byte[]{70, 114, 97, 110, -61, -89, 111, 105, 115}); // "François";
    inputStrings[2] =
        new String(new byte[]{67, -61, -76, 116, 101, 32, 100, 39, 73, 118, 111, 105, 114, 101}); // "Côte d'Ivoire";
    Arrays.sort(inputStrings);

    SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(false, inputStrings, fieldSpec, indexDir);
    dictionaryCreator.build();

    for (String inputString : inputStrings) {
      Assert.assertTrue(dictionaryCreator.indexOfSV(inputString) >= 0, "Value not found in dictionary " + inputString);
    }

    dictionaryCreator.close();
    FileUtils.deleteQuietly(indexDir);
  }

  /**
   * Tests SegmentDictionaryCreator for case when there is only one string and it is empty.
   */
  @Test
  public void testSingleEmptyString() throws Exception {
    File indexDir = new File("/tmp/dict.test");
    indexDir.deleteOnExit();
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true);

    SegmentDictionaryCreator dictionaryCreator =
        new SegmentDictionaryCreator(false, new String[]{""}, fieldSpec, indexDir);
    dictionaryCreator.build();

    Assert.assertEquals(dictionaryCreator.getStringColumnMaxLength(), 1);
    Assert.assertEquals(dictionaryCreator.indexOfSV(""), 0);

    dictionaryCreator.close();
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
    StatsCollectorConfig statsCollectorConfig = new StatsCollectorConfig(schema, null);

    switch (dataType) {
      case INT:
        return new IntColumnPreIndexStatsCollector(column, statsCollectorConfig);

      case LONG:
        return new LongColumnPreIndexStatsCollector(column, statsCollectorConfig);

      case FLOAT:
        return new FloatColumnPreIndexStatsCollector(column, statsCollectorConfig);

      case DOUBLE:
        return new DoubleColumnPreIndexStatsCollector(column, statsCollectorConfig);

      case STRING:
        return new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);

      case BOOLEAN:
        return new StringColumnPreIndexStatsCollector(column, statsCollectorConfig);

      default:
        throw new IllegalArgumentException("Illegal data type for stats builder: " + dataType);
    }
  }

}
