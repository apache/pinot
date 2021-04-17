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
import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.spi.utils.Pairs;
import org.apache.pinot.spi.utils.ReadMode;
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
      add("time_day"); // INT, cardinality 1
      add("column10"); // STRING, cardinality 27
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
    URI resourceUri = new URI(resourceUrl.toString());
    _avroFile = new File(resourceUri);

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
    IndexSegment indexSegment = ImmutableSegmentLoader.load(_segmentDirectory, indexLoadingConfig);

    // Compare the loaded inverted index with the record in avro file
    try (DataFileStream<GenericRecord> reader =
        new DataFileStream<>(new FileInputStream(_avroFile), new GenericDatumReader<>())) {
      // Check the first 1000 records
      for (int docId = 0; docId < 1000; docId++) {
        GenericRecord record = reader.next();
        for (String column : INVERTED_INDEX_COLUMNS) {
          DataSource dataSource = indexSegment.getDataSource(column);
          Dictionary dictionary = dataSource.getDictionary();
          InvertedIndexReader invertedIndex = dataSource.getInvertedIndex();

          int dictId = dictionary.indexOf(record.get(column).toString());
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
