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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@SuppressWarnings({"rawtypes", "unchecked"})
public class IntArraysTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "IntArraysTest");

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

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);

    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "weeksSinceEpochSunday",
            TimeUnit.DAYS, "test");
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    config.setSkipTimeValueCheck(true);
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
  public void test1()
      throws Exception {
    ImmutableSegment heapSegment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.heap);
    ImmutableSegment mmapSegment = ImmutableSegmentLoader.load(INDEX_DIR.listFiles()[0], ReadMode.mmap);
    Map<String, ColumnMetadata> metadataMap =
        ((SegmentMetadataImpl) heapSegment.getSegmentMetadata()).getColumnMetadataMap();

    for (String column : metadataMap.keySet()) {
      ForwardIndexReader heapForwardIndex = heapSegment.getForwardIndex(column);
      ForwardIndexReader mmapForwardIndex = mmapSegment.getForwardIndex(column);
      try (ForwardIndexReaderContext heapReaderContext = heapForwardIndex.createContext();
          ForwardIndexReaderContext mmapReaderContext = mmapForwardIndex.createContext()) {
        int numDocs = metadataMap.get(column).getTotalDocs();
        if (metadataMap.get(column).isSingleValue()) {
          for (int i = 0; i < numDocs; i++) {
            assertEquals(heapForwardIndex.getDictId(i, heapReaderContext),
                mmapForwardIndex.getDictId(i, mmapReaderContext));
          }
        } else {
          int maxNumValuesPerMVEntry = metadataMap.get(column).getMaxNumberOfMultiValues();
          int[] heapDictIdBuffer = new int[maxNumValuesPerMVEntry];
          int[] mmapDictIdBuffer = new int[maxNumValuesPerMVEntry];
          for (int i = 0; i < numDocs; i++) {
            int heapNumValues = heapForwardIndex.getDictIdMV(i, heapDictIdBuffer, heapReaderContext);
            int mmapNumValues = mmapForwardIndex.getDictIdMV(i, mmapDictIdBuffer, mmapReaderContext);
            assertEquals(heapNumValues, mmapNumValues);
            for (int j = 0; j < heapNumValues; j++) {
              assertEquals(heapDictIdBuffer[j], mmapDictIdBuffer[j]);
            }
          }
        }
      }
    }

    heapSegment.destroy();
    mmapSegment.destroy();
  }
}
