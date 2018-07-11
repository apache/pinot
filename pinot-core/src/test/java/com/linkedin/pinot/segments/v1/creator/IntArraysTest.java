/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.io.reader.DataFileReader;
import com.linkedin.pinot.core.io.reader.SingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.SingleColumnSingleValueReader;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.util.AvroUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class IntArraysTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static File INDEX_DIR =
      new File(FileUtils.getTempDirectory() + File.separator + IntArraysTest.class.getName());

  @AfterClass
  public static void cleanup() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public static void before() throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(DictionariesTest.class.getClassLoader().getResource(AVRO_DATA));
    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

//    System.out.println(INDEX_DIR.getAbsolutePath());
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR,
            "weeksSinceEpochSunday", TimeUnit.DAYS, "test");
    config.setTimeColumnName("weeksSinceEpochSunday");
    driver.init(config);
    driver.build();

    final DataFileStream<GenericRecord> avroReader = AvroUtils.getAvroReader(new File(filePath));
    final org.apache.avro.Schema avroSchema = avroReader.getSchema();
    final String[] columns = new String[avroSchema.getFields().size()];
    int i = 0;
    for (final Field f : avroSchema.getFields()) {
      columns[i] = f.name();
      i++;
    }
  }

  @Test
  public void test1() throws Exception {
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.heap);
    ImmutableSegment mmapSegment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    Map<String, ColumnMetadata> metadataMap =
        ((SegmentMetadataImpl) heapSegment.getSegmentMetadata()).getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      DataFileReader heapArray = heapSegment.getForwardIndex(column);
      DataFileReader mmapArray = mmapSegment.getForwardIndex(column);

      if (metadataMap.get(column).isSingleValue()) {
        final SingleColumnSingleValueReader svHeapReader = (SingleColumnSingleValueReader) heapArray;
        final SingleColumnSingleValueReader mvMmapReader = (SingleColumnSingleValueReader) mmapArray;
        for (int i = 0; i < metadataMap.get(column).getTotalDocs(); i++) {
          Assert.assertEquals(mvMmapReader.getInt(i), svHeapReader.getInt(i));
        }
      } else {
        final SingleColumnMultiValueReader svHeapReader = (SingleColumnMultiValueReader) heapArray;
        final SingleColumnMultiValueReader mvMmapReader = (SingleColumnMultiValueReader) mmapArray;
        for (int i = 0; i < metadataMap.get(column).getTotalDocs(); i++) {
          final int[] i_1 = new int[1000];
          final int[] j_i = new int[1000];
          Assert.assertEquals(mvMmapReader.getIntArray(i, j_i), svHeapReader.getIntArray(i, i_1));
        }
      }
    }
  }
}
