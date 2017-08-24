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
import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.io.FileUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class BitmapInvertedIndexTest {
  private static final String AVRO_FILE_PATH = "data" + File.separator + "test_sample_data.avro";
  private static final File INDEX_DIR =
      new File(FileUtils.getTempDirectory(), BitmapInvertedIndexTest.class.getSimpleName());
  private static final Set<String> INVERTED_INDEX_COLUMNS = new HashSet<String>(3) {
    {
      add("time_day");            // INT, cardinality 1
      add("column10");            // STRING, cardinality 27
      add("met_impressionCount"); // LONG, cardinality 21
    }
  };

  private File _avroFile;
  private File _segmentDirectory;

  @BeforeClass
  public void setUp() throws Exception {
    FileUtils.deleteQuietly(INDEX_DIR);
    URL resourceUrl = getClass().getClassLoader().getResource(AVRO_FILE_PATH);
    Assert.assertNotNull(resourceUrl);
    _avroFile = new File(resourceUrl.getFile());

    SegmentGeneratorConfig segmentGeneratorConfig =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(_avroFile, INDEX_DIR, "myTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig);
    driver.build();

    _segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
  }

  @Test
  public void testBitmapInvertedIndex() throws Exception {
    testBitmapInvertedIndex(ReadMode.heap);
    testBitmapInvertedIndex(ReadMode.mmap);
  }

  private void testBitmapInvertedIndex(ReadMode readMode) throws Exception {
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setReadMode(readMode);
    indexLoadingConfig.setInvertedIndexColumns(INVERTED_INDEX_COLUMNS);
    IndexSegment indexSegment = ColumnarSegmentLoader.load(_segmentDirectory, indexLoadingConfig);

    // Compare the loaded inverted index with the record in avro file
    try (DataFileStream<GenericRecord> reader = new DataFileStream<>(new FileInputStream(_avroFile),
        new GenericDatumReader<GenericRecord>())) {
      // Check the first 1000 records
      for (int docId = 0; docId < 1000; docId++) {
        GenericRecord record = reader.next();
        for (String column : INVERTED_INDEX_COLUMNS) {
          Object entry = record.get(column);
          if (entry instanceof Utf8) {
            entry = entry.toString();
          }
          DataSource dataSource = indexSegment.getDataSource(column);
          Dictionary dictionary = dataSource.getDictionary();
          InvertedIndexReader invertedIndex = dataSource.getInvertedIndex();

          int dictId = dictionary.indexOf(entry);
          int size = dictionary.length();
          if (dataSource.getDataSourceMetadata().isSorted()) {
            for (int i = 0; i < size; i++) {
              Pairs.IntPair minMaxRange = (Pairs.IntPair) invertedIndex.getDocIds(i);
              int min = minMaxRange.getLeft();
              int max = minMaxRange.getRight();
              if (i == dictId) {
                Assert.assertTrue(docId >= min && docId < max);
              } else {
                Assert.assertTrue(docId < min || docId >= max);
              }
            }
          } else {
            for (int i = 0; i < size; i++) {
              ImmutableRoaringBitmap immutableRoaringBitmap = (ImmutableRoaringBitmap) invertedIndex.getDocIds(i);
              if (i == dictId) {
                Assert.assertTrue(immutableRoaringBitmap.contains(docId));
              } else {
                Assert.assertFalse(immutableRoaringBitmap.contains(docId));
              }
            }
          }
        }
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }
}
