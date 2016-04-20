/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.segment.creator.AbstractColumnStatisticsCollector;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.SegmentDictionaryCreator;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
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

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    final String filePath = TestUtils
        .getFileFromResourceUrl(DictionariesTest.class.getClassLoader().getResource(AVRO_DATA));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            TimeUnit.DAYS, "test");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();

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
  public void test1() throws Exception {
    final IndexSegmentImpl heapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    final IndexSegmentImpl mmapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);

    for (final String column : ((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap().keySet()) {
      final ImmutableDictionaryReader heapDictionary = heapSegment.getDictionaryFor(column);
      final ImmutableDictionaryReader mmapDictionary = mmapSegment.getDictionaryFor(column);

      switch (((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap().get(column).getDataType()) {
        case BOOLEAN:
        case STRING:
          Assert.assertEquals(true, heapDictionary instanceof StringDictionary);
          Assert.assertEquals(true, mmapDictionary instanceof StringDictionary);
          break;
        case DOUBLE:
          Assert.assertEquals(true, heapDictionary instanceof DoubleDictionary);
          Assert.assertEquals(true, mmapDictionary instanceof DoubleDictionary);
          break;
        case FLOAT:
          Assert.assertEquals(true, heapDictionary instanceof FloatDictionary);
          Assert.assertEquals(true, mmapDictionary instanceof FloatDictionary);
          break;
        case LONG:
          Assert.assertEquals(true, heapDictionary instanceof LongDictionary);
          Assert.assertEquals(true, mmapDictionary instanceof LongDictionary);
          break;
        case INT:
          Assert.assertEquals(true, heapDictionary instanceof IntDictionary);
          Assert.assertEquals(true, mmapDictionary instanceof IntDictionary);
          break;
      }

      Assert.assertEquals(mmapDictionary.length(), heapDictionary.length());
      for (int i = 0; i < heapDictionary.length(); i++) {
        Assert.assertEquals(mmapDictionary.get(i), heapDictionary.get(i));
      }
    }
  }

  @Test
  public void test2() throws Exception {
    final IndexSegmentImpl heapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    final IndexSegmentImpl mmapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);

    final Map<String, ColumnMetadata> metadataMap = ((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap();
    for (final String column : metadataMap.keySet()) {
      final ImmutableDictionaryReader heapDictionary = heapSegment.getDictionaryFor(column);
      final ImmutableDictionaryReader mmapDictionary = mmapSegment.getDictionaryFor(column);

      final Set<Object> uniques = uniqueEntries.get(column);
      final List<Object> list = Arrays.asList(uniques.toArray());
      Collections.shuffle(list);
      for (final Object entry : list) {
        Assert.assertEquals(mmapDictionary.indexOf(entry), heapDictionary.indexOf(entry));
        if (!column.equals("pageKey")) {
          Assert.assertEquals(false, heapDictionary.indexOf(entry) < 0);
          Assert.assertEquals(false, mmapDictionary.indexOf(entry) < 0);
        }
      }
    }
  }

  @Test
  public void testIntColumnPreIndexStatsCollector() throws Exception {
    FieldSpec spec = new DimensionFieldSpec("column1", DataType.INT, true);
    AbstractColumnStatisticsCollector statsCollector = new IntColumnPreIndexStatsCollector(spec);
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
  public void testFloatColumnPreIndexStatsCollector() throws Exception {
    FieldSpec spec = new DimensionFieldSpec("column1", DataType.FLOAT, true);
    AbstractColumnStatisticsCollector statsCollector = new FloatColumnPreIndexStatsCollector(spec);
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
  public void testLongColumnPreIndexStatsCollector() throws Exception {
    FieldSpec spec = new DimensionFieldSpec("column1", DataType.LONG, true);
    AbstractColumnStatisticsCollector statsCollector = new LongColumnPreIndexStatsCollector(spec);
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
  public void testDoubleColumnPreIndexStatsCollector() throws Exception {
    FieldSpec spec = new DimensionFieldSpec("column1", DataType.DOUBLE, true);
    AbstractColumnStatisticsCollector statsCollector = new DoubleColumnPreIndexStatsCollector(spec);
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
  public void testStringColumnPreIndexStatsCollectorForRandomString() throws Exception {
    FieldSpec spec = new DimensionFieldSpec("column1", DataType.STRING, true);
    AbstractColumnStatisticsCollector statsCollector = new StringColumnPreIndexStatsCollector(spec);
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
  public void testStringColumnPreIndexStatsCollectorForBoolean() throws Exception {
    FieldSpec spec = new DimensionFieldSpec("column1", DataType.BOOLEAN, true);
    AbstractColumnStatisticsCollector statsCollector = new StringColumnPreIndexStatsCollector(spec);
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
   * Tests DictionaryCreator for case when one value is a substring of another.
   * For example, in case of sorted values {"abc", "abc def"} after padding,
   * the sorted order would change to {"abc def%%%%", "abc%%%%%%%"}
   *
   * This test asserts that DictionaryCreator.indexOfSV("abc") returns 1 (ie index of "abc%%%%%%%"
   * in actual padded dictionary), and not 0.
   *
   * @throws Exception
   */
  @Test
  public void testStringsValuesWithPadding()
      throws Exception {
    File indexDir = new File("/tmp/dict.test");
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true, "\t");

    String[] inputStrings = new String[2];
    String[] paddedStrings = new String[2];

    inputStrings[0] = "abc def";
    inputStrings[1] = "abc";
    Arrays.sort(inputStrings); // Sorted order: {"abc", "abc def"}

    SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(false, inputStrings, fieldSpec, indexDir);
    dictionaryCreator.build();

    // Get the padded string as stored in the dictionary.
    int targetPaddedLength = dictionaryCreator.getStringColumnMaxLength();
    for (int i = 0; i < inputStrings.length; i++) {
      paddedStrings[i] = SegmentDictionaryCreator.getPaddedString(inputStrings[i], targetPaddedLength);
    }
    Arrays.sort(paddedStrings); // Sorted Order: {"abc def%%%%", "abc%%%%%%%"}

    // Assert that indexOfSV for un-padded string returns the index of the corresponding padded string.
    for (int i = 0; i < inputStrings.length; i++) {
      int paddedIndex = dictionaryCreator.indexOfSV(inputStrings[i]);
      Assert.assertTrue(paddedStrings[paddedIndex]
          .equals(SegmentDictionaryCreator.getPaddedString(inputStrings[i], targetPaddedLength)));
    }

    dictionaryCreator.close();
    FileUtils.deleteQuietly(indexDir);
  }

  /**
   * Tests SegmentDictionaryCreator for case when there is only one string
   * and it is empty
   *
   * This test asserts that the padded length of the empty string is 1
   * in actual padded dictionary), and not 0.
   *
   * @throws Exception
   */
  @Test
  public void testSingleEmptyString()
      throws Exception {
    File indexDir = new File("/tmp/dict.test");
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true, "\t");

    String[] inputStrings = new String[1];
    String[] paddedStrings = new String[1];

    try {
      inputStrings[0] = "";
      Arrays.sort(inputStrings); // Sorted order: {""}

      SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(false, inputStrings, fieldSpec, indexDir);
      dictionaryCreator.build();

      // Get the padded string as stored in the dictionary.
      int targetPaddedLength = dictionaryCreator.getStringColumnMaxLength();
      Assert.assertTrue(targetPaddedLength == 1);
      for (int i = 0; i < inputStrings.length; i++) {
        paddedStrings[i] = SegmentDictionaryCreator.getPaddedString(inputStrings[i], targetPaddedLength);
      }
      Arrays.sort(paddedStrings); // Sorted Order: {"%"}

      // Assert that indexOfSV for un-padded string returns the index of the corresponding padded string.
      for (int i = 0; i < inputStrings.length; i++) {
        int paddedIndex = dictionaryCreator.indexOfSV(inputStrings[i]);
        Assert.assertTrue(paddedStrings[paddedIndex].equals(
            SegmentDictionaryCreator.getPaddedString(inputStrings[i], targetPaddedLength)));
      }

      // Verify that empty string got padded
      Assert.assertTrue(paddedStrings[0].equals("%"));
      dictionaryCreator.close();
    } catch (Exception e) {
        throw e;
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }
  /**
   * Tests SegmentDictionaryCreator for case when there is only one string
   * and it is "null"
   *
   * This test asserts that the padded length of the null string is 4
   *
   * @throws Exception
   */
  @Test
  public void testSingleNullString()
      throws Exception {
    File indexDir = new File("/tmp/dict.test");
    FieldSpec fieldSpec = new DimensionFieldSpec("test", DataType.STRING, true, "\t");

    String[] inputStrings = new String[1];
    String[] paddedStrings = new String[1];

    inputStrings[0] = "null";
    Arrays.sort(inputStrings); // Sorted order: {"null"}

    try {
      SegmentDictionaryCreator dictionaryCreator = new SegmentDictionaryCreator(false, inputStrings, fieldSpec, indexDir);
      dictionaryCreator.build();

      // Get the padded string as stored in the dictionary.
      int targetPaddedLength = dictionaryCreator.getStringColumnMaxLength();
      Assert.assertTrue(targetPaddedLength == 4);
      for (int i = 0; i < inputStrings.length; i++) {
        paddedStrings[i] = SegmentDictionaryCreator.getPaddedString(inputStrings[i], targetPaddedLength);
      }
      Arrays.sort(paddedStrings); // Sorted Order: {"null"}

      // Assert that indexOfSV for un-padded string returns the index of the corresponding padded string.
      for (int i = 0; i < inputStrings.length; i++) {
        int paddedIndex = dictionaryCreator.indexOfSV(inputStrings[i]);
        Assert.assertTrue(paddedStrings[paddedIndex].equals(
            SegmentDictionaryCreator.getPaddedString(inputStrings[i], targetPaddedLength)));
      }

      // Verify that the string "null" did not get changed
      Assert.assertTrue(paddedStrings[0].equals("null"));
      dictionaryCreator.close();
    } catch (Exception e) {
      throw e;
    } finally {
      FileUtils.deleteQuietly(indexDir);
    }
  }
}
