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
package com.linkedin.pinot.core.indexsegment.mutable;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.BlockMultiValIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.DataSourceMetadata;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.AvroRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import java.io.File;
import java.net.URL;
import java.util.Collections;
import org.apache.commons.io.FileUtils;
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

  @BeforeClass
  public void setUp() throws Exception {
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
    _mutableSegmentImpl =
        MutableSegmentImplTestUtils.createMutableSegmentImpl(_schema, Collections.emptySet(), Collections.emptySet(),
            false);
    try (RecordReader recordReader = new AvroRecordReader(avroFile, _schema)) {
      GenericRow reuse = new GenericRow();
      while (recordReader.hasNext()) {
        _mutableSegmentImpl.index(recordReader.next(reuse));
      }
    }
  }

  @Test
  public void testMetadata() {
    SegmentMetadata actualSegmentMetadata = _mutableSegmentImpl.getSegmentMetadata();
    SegmentMetadata expectedSegmentMetadata = _immutableSegment.getSegmentMetadata();
    Assert.assertEquals(actualSegmentMetadata.getTotalDocs(), expectedSegmentMetadata.getTotalDocs());

    for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
      String column = fieldSpec.getName();
      DataSourceMetadata actualDataSourceMetadata = _mutableSegmentImpl.getDataSource(column).getDataSourceMetadata();
      DataSourceMetadata expectedDataSourceMetadata = _immutableSegment.getDataSource(column).getDataSourceMetadata();
      Assert.assertEquals(actualDataSourceMetadata.getDataType(), expectedDataSourceMetadata.getDataType());
      Assert.assertEquals(actualDataSourceMetadata.isSingleValue(), expectedDataSourceMetadata.isSingleValue());
      Assert.assertEquals(actualDataSourceMetadata.getNumDocs(), expectedDataSourceMetadata.getNumDocs());
      if (!expectedDataSourceMetadata.isSingleValue()) {
        Assert.assertEquals(actualDataSourceMetadata.getMaxNumMultiValues(),
            expectedDataSourceMetadata.getMaxNumMultiValues());
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
          Assert.assertEquals(actualDictionary.get(actualDictId), expectedDictionary.get(expectedDictId));
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

        int numMaxMultiValues = expectedDataSource.getDataSourceMetadata().getMaxNumMultiValues();
        int[] actualDictIds = new int[numMaxMultiValues];
        int[] expectedDictIds = new int[numMaxMultiValues];

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
