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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BitmapInvertedIndexTest {
  private static final String AVRO_DATA = "data/test_sample_data.avro";
  private static final File INDEX_DIR = new File(BitmapInvertedIndexTest.class.toString());

  private File _segmentDirectory;
  private Set<String> _invertedIndexColumns;

  @Test
  public void test1() throws Exception {
    // load segment in heap mode
   testBitMapInvertedIndex(ReadMode.heap);
  }

  @Test
  public void test2() throws Exception {
    testBitMapInvertedIndex(ReadMode.mmap);
  }

  void testBitMapInvertedIndex(ReadMode readMode)
      throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setReadMode(readMode);
    indexLoadingConfig.setInvertedIndexColumns(_invertedIndexColumns);
    IndexSegmentImpl mmapSegment = (IndexSegmentImpl) ColumnarSegmentLoader.load(_segmentDirectory, indexLoadingConfig);

    // compare the loaded inverted index with the record in avro file
    final DataFileStream<GenericRecord> reader =
        new DataFileStream<GenericRecord>(new FileInputStream(new File(getClass().getClassLoader()
            .getResource(AVRO_DATA).getFile())), new GenericDatumReader<GenericRecord>());
    int docId = 0;
    while (reader.hasNext()) {
      final GenericRecord rec = reader.next();
      for (final String column : ((SegmentMetadataImpl) mmapSegment.getSegmentMetadata()).getColumnMetadataMap().keySet()) {
        Object entry = rec.get(column);
        if (entry instanceof Utf8) {
          entry = ((Utf8) entry).toString();
        }
        final int dicId = mmapSegment.getDictionaryFor(column).indexOf(entry);
        // make sure that docId for dicId exist in the inverted index
        Assert.assertTrue(mmapSegment.getInvertedIndexFor(column).getImmutable(dicId).contains(docId));
        final int size = mmapSegment.getDictionaryFor(column).length();
        for (int i = 0; i < size; ++i) { // remove this for-loop for quick test
          if (i == dicId) {
            continue;
          }
          // make sure that docId for dicId does not exist in the inverted index
          Assert.assertFalse(mmapSegment.getInvertedIndexFor(column).getImmutable(i).contains(docId));
        }
      }
      ++docId;
    }
  }

  @AfterClass
  public void teardown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public void setup() throws Exception {
    final String filePath = TestUtils.getFileFromResourceUrl(getClass().getClassLoader().getResource(AVRO_DATA));

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    final SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "time_day",
            TimeUnit.DAYS, "test");

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
    _invertedIndexColumns = new HashSet<>(config.getInvertedIndexCreationColumns());
  }
}
