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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link com.linkedin.pinot.core.segment.index.readers.OnHeapStringDictionary} class.
 */
public class OnHeapStringDictionaryTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(OnHeapStringDictionaryTest.class);
  private static final int NUM_ROWS = 1001;

  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "onHeapDict";
  private static final String SEGMENT_NAME = "onHeapDictSeg";
  private static final int MAX_STRING_LENGTH = 101;
  private static final String COLUMN_NAME = "column";

  /**
   * Builds a segment with one string column, and loads two version of its dictionaries, one with default
   * off-heap {@link com.linkedin.pinot.core.segment.index.readers.StringDictionary} and another with
   * {@link com.linkedin.pinot.core.segment.index.readers.OnHeapStringDictionary}.
   *
   * Tests all interfaces return the same result for the two dictionary implementations.
   *
   * @throws Exception
   */
  @Test
  public void test()
      throws Exception {

    buildSegment(SEGMENT_DIR_NAME, SEGMENT_NAME);
    File indexDir = new File(SEGMENT_DIR_NAME, SEGMENT_NAME);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(COLUMN_NAME);
    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap);
    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();

    ImmutableDictionaryReader offHeapDictionary =
        ColumnIndexContainer.loadDictionary(columnMetadata, segmentReader, false);
    ImmutableDictionaryReader onHeapDictionary =
        ColumnIndexContainer.loadDictionary(columnMetadata, segmentReader, true);

    int numElements = offHeapDictionary.length();
    Assert.assertEquals(onHeapDictionary.length(), numElements, "Dictionary length mis-match");
    for (int id = 0; id < numElements; id++) {
      String expected = (String) offHeapDictionary.get(id);
      Assert.assertEquals(onHeapDictionary.get(id), expected);
      Assert.assertEquals(onHeapDictionary.getStringValue(id), offHeapDictionary.getStringValue(id));
      Assert.assertEquals(onHeapDictionary.indexOf(expected), id);
    }

    Random random = new Random(System.nanoTime());
    int batchSize = random.nextInt(onHeapDictionary.length());
    int dictIds[] = new int[batchSize];

    String[] actualValues = new String[batchSize];
    String[] expectedValues = new String[batchSize];

    for (int i = 0; i < 100; i++) {
      for (int j = 0; j < batchSize; j++) {
        dictIds[j] = random.nextInt(numElements);
      }
      onHeapDictionary.readStringValues(dictIds, 0, batchSize, actualValues, 0);
      offHeapDictionary.readStringValues(dictIds, 0, batchSize, expectedValues, 0);
      Assert.assertEquals(actualValues, expectedValues);
    }

    segmentReader.close();
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    FileUtils.deleteDirectory(new File(SEGMENT_DIR_NAME));
  }

  /**
   * Helper method to build a segment with one String column.
   *
   * @param segmentDirName Name of segment directory
   * @param segmentName Name of segment
   * @throws Exception
   */
  private void buildSegment(String segmentDirName, String segmentName)
      throws Exception {
    // Build schema with two string columns:
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true));

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);

    Random random = new Random(System.nanoTime());
    List<GenericRow> data = new ArrayList<>();

    for (int i = 0; i < NUM_ROWS; i++) {
      String value = RandomStringUtils.random(random.nextInt(MAX_STRING_LENGTH));

      Map<String, Object> map = new HashMap<>();
      for (String column : schema.getColumnNames()) {
        map.put(column, value);
      }
      GenericRow row = new GenericRow();
      row.init(map);
      data.add(row);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader reader = new TestUtils.GenericRowRecordReader(schema, data);
    driver.init(config, reader);
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
  }
}
