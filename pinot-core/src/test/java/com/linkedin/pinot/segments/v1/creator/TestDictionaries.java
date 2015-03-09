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
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;


public class TestDictionaries {
  private static final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestDictionaries.class.toString());
  static Map<String, Set<Object>> uniqueEntries;

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    final String filePath = TestUtils
        .getFileFromResourceUrl(TestDictionaries.class.getClassLoader().getResource(AVRO_DATA));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            TimeUnit.DAYS, "test", "testTable");



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

    for (final String column : ((SegmentMetadataImpl)mmapSegment.getSegmentMetadata()).getColumnMetadataMap().keySet()) {
      final ImmutableDictionaryReader heapDictionary = heapSegment.getDictionaryFor(column);
      final ImmutableDictionaryReader mmapDictionary = mmapSegment.getDictionaryFor(column);

      switch (((SegmentMetadataImpl)mmapSegment.getSegmentMetadata()).getColumnMetadataMap().get(column).getDataType()) {
        case BOOLEAN:
        case STRING:
          AssertJUnit.assertEquals(heapDictionary instanceof StringDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof StringDictionary, true);
          break;
        case DOUBLE:
          AssertJUnit.assertEquals(heapDictionary instanceof DoubleDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof DoubleDictionary, true);
          break;
        case FLOAT:
          AssertJUnit.assertEquals(heapDictionary instanceof FloatDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof FloatDictionary, true);
          break;
        case LONG:
          AssertJUnit.assertEquals(heapDictionary instanceof LongDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof LongDictionary, true);
          break;
        case INT:
          AssertJUnit.assertEquals(heapDictionary instanceof IntDictionary, true);
          AssertJUnit.assertEquals(mmapDictionary instanceof IntDictionary, true);
          break;
      }

      AssertJUnit.assertEquals(heapDictionary.length(), mmapDictionary.length());
      for (int i = 0; i < heapDictionary.length(); i++) {
        AssertJUnit.assertEquals(heapDictionary.get(i), mmapDictionary.get(i));
      }
    }
  }

  @Test
  public void test2() throws Exception {
    final IndexSegmentImpl heapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    final IndexSegmentImpl mmapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.mmap);

    final Map<String, ColumnMetadata> metadataMap = ((SegmentMetadataImpl)mmapSegment.getSegmentMetadata()).getColumnMetadataMap();
    for (final String column : metadataMap.keySet()) {
      final ImmutableDictionaryReader heapDictionary = heapSegment.getDictionaryFor(column);
      final ImmutableDictionaryReader mmapDictionary = mmapSegment.getDictionaryFor(column);

      final Set<Object> uniques = uniqueEntries.get(column);
      final List<Object> list = Arrays.asList(uniques.toArray());
      Collections.shuffle(list);
      for (final Object entry : list) {
        AssertJUnit.assertEquals(heapDictionary.indexOf(entry), mmapDictionary.indexOf(entry));
        if (!column.equals("pageKey")) {
          AssertJUnit.assertEquals(heapDictionary.indexOf(entry) < 0, false);
          AssertJUnit.assertEquals(mmapDictionary.indexOf(entry) < 0, false);
        }
      }
    }
  }
}
