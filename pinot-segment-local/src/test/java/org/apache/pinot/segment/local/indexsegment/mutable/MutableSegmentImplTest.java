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
package org.apache.pinot.segment.local.indexsegment.mutable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.SegmentTestUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentIndexCreationDriver;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.ReadMode;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


@SuppressWarnings({"rawtypes", "unchecked"})
public class MutableSegmentImplTest {
  private static final String AVRO_FILE = "data/test_data-mv.avro";
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "MutableSegmentImplTest");

  private Schema _schema;
  private MutableSegmentImpl _mutableSegmentImpl;
  private ImmutableSegment _immutableSegment;
  private long _lastIndexedTs;
  private long _lastIngestionTimeMs;
  private long _startTimeMs;

  @BeforeClass
  public void setUp()
      throws Exception {
    FileUtils.deleteQuietly(TEMP_DIR);

    URL resourceUrl = MutableSegmentImplTest.class.getClassLoader().getResource(AVRO_FILE);
    Assert.assertNotNull(resourceUrl);
    File avroFile = new File(resourceUrl.getFile());

    SegmentGeneratorConfig config =
        SegmentTestUtils.getSegmentGeneratorConfigWithoutTimeColumn(avroFile, TEMP_DIR, "testTable");
    SegmentIndexCreationDriver driver = new SegmentIndexCreationDriverImpl();
    driver.init(config);
    driver.build();
    _immutableSegment = ImmutableSegmentLoader.load(new File(TEMP_DIR, driver.getSegmentName()), ReadMode.mmap);

    _schema = config.getSchema();
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(_schema, "testSegment");
    _mutableSegmentImpl = MutableSegmentImplTestUtils
        .createMutableSegmentImpl(_schema, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
            false);
    _lastIngestionTimeMs = System.currentTimeMillis();
    StreamMessageMetadata defaultMetadata = new StreamMessageMetadata(_lastIngestionTimeMs);
    _startTimeMs = System.currentTimeMillis();

    try (RecordReader recordReader = RecordReaderFactory
        .getRecordReader(FileFormat.AVRO, avroFile, _schema.getColumnNames(), null)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        _mutableSegmentImpl.index(recordReader.next(reuse), defaultMetadata);
        _lastIndexedTs = System.currentTimeMillis();
      }
    }
  }

  @Test
  public void testMetadata() {
    SegmentMetadata actualSegmentMetadata = _mutableSegmentImpl.getSegmentMetadata();
    SegmentMetadata expectedSegmentMetadata = _immutableSegment.getSegmentMetadata();
    assertEquals(actualSegmentMetadata.getTotalDocs(), expectedSegmentMetadata.getTotalDocs());

    // assert that the last indexed timestamp is close to what we expect
    long actualTs = _mutableSegmentImpl.getSegmentMetadata().getLastIndexedTimestamp();
    Assert.assertTrue(actualTs >= _startTimeMs);
    Assert.assertTrue(actualTs <= _lastIndexedTs);

    assertEquals(_mutableSegmentImpl.getSegmentMetadata().getLatestIngestionTimestamp(), _lastIngestionTimeMs);

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      DataSourceMetadata actualDataSourceMetadata = _mutableSegmentImpl.getDataSource(column).getDataSourceMetadata();
      DataSourceMetadata expectedDataSourceMetadata = _immutableSegment.getDataSource(column).getDataSourceMetadata();
      assertEquals(actualDataSourceMetadata.getDataType(), expectedDataSourceMetadata.getDataType());
      assertEquals(actualDataSourceMetadata.isSingleValue(), expectedDataSourceMetadata.isSingleValue());
      assertEquals(actualDataSourceMetadata.getNumDocs(), expectedDataSourceMetadata.getNumDocs());
      if (!expectedDataSourceMetadata.isSingleValue()) {
        assertEquals(actualDataSourceMetadata.getMaxNumValuesPerMVEntry(),
            expectedDataSourceMetadata.getMaxNumValuesPerMVEntry());
      }
    }
  }

  @Test
  public void testDataSourceForSVColumns()
      throws IOException {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.isSingleValueField()) {
        String column = fieldSpec.getName();
        DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
        DataSource expectedDataSource = _immutableSegment.getDataSource(column);

        int actualNumDocs = actualDataSource.getDataSourceMetadata().getNumDocs();
        int expectedNumDocs = expectedDataSource.getDataSourceMetadata().getNumDocs();
        assertEquals(actualNumDocs, expectedNumDocs);

        Dictionary actualDictionary = actualDataSource.getDictionary();
        Dictionary expectedDictionary = expectedDataSource.getDictionary();
        assertEquals(actualDictionary.length(), expectedDictionary.length());

        // Allow the segment name to be different
        if (column.equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
          continue;
        }

        ForwardIndexReader actualReader = actualDataSource.getForwardIndex();
        ForwardIndexReader expectedReader = expectedDataSource.getForwardIndex();
        try (ForwardIndexReaderContext actualReaderContext = actualReader.createContext();
            ForwardIndexReaderContext expectedReaderContext = expectedReader.createContext()) {
          for (int docId = 0; docId < expectedNumDocs; docId++) {
            int actualDictId = actualReader.getDictId(docId, actualReaderContext);
            int expectedDictId = expectedReader.getDictId(docId, expectedReaderContext);
            assertEquals(actualDictionary.get(actualDictId), expectedDictionary.get(expectedDictId));
          }
        }
      }
    }
  }

  @Test
  public void testDataSourceForMVColumns()
      throws IOException {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        String column = fieldSpec.getName();
        DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
        DataSource expectedDataSource = _immutableSegment.getDataSource(column);

        int actualNumDocs = actualDataSource.getDataSourceMetadata().getNumDocs();
        int expectedNumDocs = expectedDataSource.getDataSourceMetadata().getNumDocs();
        assertEquals(actualNumDocs, expectedNumDocs);

        Dictionary actualDictionary = actualDataSource.getDictionary();
        Dictionary expectedDictionary = expectedDataSource.getDictionary();
        assertEquals(actualDictionary.length(), expectedDictionary.length());

        int maxNumValuesPerMVEntry = expectedDataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry();
        int[] actualDictIds = new int[maxNumValuesPerMVEntry];
        int[] expectedDictIds = new int[maxNumValuesPerMVEntry];

        ForwardIndexReader actualReader = actualDataSource.getForwardIndex();
        ForwardIndexReader expectedReader = expectedDataSource.getForwardIndex();
        try (ForwardIndexReaderContext actualReaderContext = actualReader.createContext();
            ForwardIndexReaderContext expectedReaderContext = expectedReader.createContext()) {
          for (int docId = 0; docId < expectedNumDocs; docId++) {
            int actualLength = actualReader.getDictIdMV(docId, actualDictIds, actualReaderContext);
            int expectedLength = expectedReader.getDictIdMV(docId, expectedDictIds, expectedReaderContext);
            assertEquals(actualLength, expectedLength);

            for (int i = 0; i < expectedLength; i++) {
              assertEquals(actualDictionary.get(actualDictIds[i]), expectedDictionary.get(expectedDictIds[i]));
            }
          }
        }
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
