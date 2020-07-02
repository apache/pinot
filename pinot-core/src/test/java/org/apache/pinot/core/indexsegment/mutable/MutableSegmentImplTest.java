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
package org.apache.pinot.core.indexsegment.mutable;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.CommonConstants;
import org.apache.pinot.core.common.BlockMultiValIterator;
import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.common.DataSourceMetadata;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.data.readers.RecordReaderFactory;
import org.apache.pinot.spi.stream.StreamMessageMetadata;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


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
    Assert.assertEquals(actualSegmentMetadata.getTotalDocs(), expectedSegmentMetadata.getTotalDocs());

    // assert that the last indexed timestamp is close to what we expect
    long actualTs = _mutableSegmentImpl.getSegmentMetadata().getLastIndexedTimestamp();
    Assert.assertTrue(actualTs >= _startTimeMs);
    Assert.assertTrue(actualTs <= _lastIndexedTs);

    Assert.assertEquals(_mutableSegmentImpl.getSegmentMetadata().getLatestIngestionTimestamp(), _lastIngestionTimeMs);

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      DataSourceMetadata actualDataSourceMetadata = _mutableSegmentImpl.getDataSource(column).getDataSourceMetadata();
      DataSourceMetadata expectedDataSourceMetadata = _immutableSegment.getDataSource(column).getDataSourceMetadata();
      Assert.assertEquals(actualDataSourceMetadata.getDataType(), expectedDataSourceMetadata.getDataType());
      Assert.assertEquals(actualDataSourceMetadata.isSingleValue(), expectedDataSourceMetadata.isSingleValue());
      Assert.assertEquals(actualDataSourceMetadata.getNumDocs(), expectedDataSourceMetadata.getNumDocs());
      if (!expectedDataSourceMetadata.isSingleValue()) {
        Assert.assertEquals(actualDataSourceMetadata.getMaxNumValuesPerMVEntry(),
            expectedDataSourceMetadata.getMaxNumValuesPerMVEntry());
      }
    }
  }

  @Test
  public void testDataSourceForSVColumns() {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (fieldSpec.isSingleValueField()) {
        String column = fieldSpec.getName();
        DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
        DataSource expectedDataSource = _immutableSegment.getDataSource(column);

        Dictionary actualDictionary = actualDataSource.getDictionary();
        Dictionary expectedDictionary = expectedDataSource.getDictionary();
        Assert.assertEquals(actualDictionary.length(), expectedDictionary.length());

        BlockSingleValIterator actualSVIterator =
            (BlockSingleValIterator) actualDataSource.nextBlock().getBlockValueSet().iterator();
        BlockSingleValIterator expectedSVIterator =
            (BlockSingleValIterator) expectedDataSource.nextBlock().getBlockValueSet().iterator();

        while (expectedSVIterator.hasNext()) {
          Assert.assertTrue(actualSVIterator.hasNext());

          int actualDictId = actualSVIterator.nextIntVal();
          int expectedDictId = expectedSVIterator.nextIntVal();
          // Only allow the default segment name to be different
          if (!column.equals(CommonConstants.Segment.BuiltInVirtualColumn.SEGMENTNAME)) {
            Assert.assertEquals(actualDictionary.get(actualDictId), expectedDictionary.get(expectedDictId));
          }
        }
        Assert.assertFalse(actualSVIterator.hasNext());
      }
    }
  }

  @Test
  public void testDataSourceForMVColumns() {
    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      if (!fieldSpec.isSingleValueField()) {
        String column = fieldSpec.getName();
        DataSource actualDataSource = _mutableSegmentImpl.getDataSource(column);
        DataSource expectedDataSource = _immutableSegment.getDataSource(column);

        Dictionary actualDictionary = actualDataSource.getDictionary();
        Dictionary expectedDictionary = expectedDataSource.getDictionary();
        Assert.assertEquals(actualDictionary.length(), expectedDictionary.length());

        BlockMultiValIterator actualMVIterator =
            (BlockMultiValIterator) actualDataSource.nextBlock().getBlockValueSet().iterator();
        BlockMultiValIterator expectedMVIterator =
            (BlockMultiValIterator) expectedDataSource.nextBlock().getBlockValueSet().iterator();

        int maxNumValuesPerMVEntry = expectedDataSource.getDataSourceMetadata().getMaxNumValuesPerMVEntry();
        int[] actualDictIds = new int[maxNumValuesPerMVEntry];
        int[] expectedDictIds = new int[maxNumValuesPerMVEntry];

        while (expectedMVIterator.hasNext()) {
          Assert.assertTrue(actualMVIterator.hasNext());

          int actualNumMultiValues = actualMVIterator.nextIntVal(actualDictIds);
          int expectedNumMultiValues = expectedMVIterator.nextIntVal(expectedDictIds);
          Assert.assertEquals(actualNumMultiValues, expectedNumMultiValues);

          for (int i = 0; i < expectedNumMultiValues; i++) {
            Assert.assertEquals(actualDictionary.get(actualDictIds[i]), expectedDictionary.get(expectedDictIds[i]));
          }
        }
        Assert.assertFalse(actualMVIterator.hasNext());
      }
    }
  }

  @AfterClass
  public void tearDown() {
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
