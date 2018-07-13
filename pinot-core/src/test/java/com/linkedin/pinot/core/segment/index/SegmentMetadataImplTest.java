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
package com.linkedin.pinot.core.segment.index;

import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.SegmentIndexCreationDriver;
import com.linkedin.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import com.linkedin.pinot.segments.v1.creator.SegmentTestUtils;
import com.linkedin.pinot.util.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class SegmentMetadataImplTest {
 private static final String AVRO_DATA = "data/test_data-mv.avro";
  private File INDEX_DIR;
  private File segmentDirectory;

  @BeforeMethod
  public void setUp()
      throws Exception {
    INDEX_DIR = Files.createTempDirectory(SegmentMetadataImplTest.class.getName() + "_segmentDir").toFile();

    final String filePath =
        TestUtils.getFileFromResourceUrl(SegmentMetadataImplTest.class.getClassLoader().getResource(AVRO_DATA));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch", TimeUnit.HOURS,
            "testTable");
    config.setSegmentNamePostfix("1");
    config.setTimeColumnName("daysSinceEpoch");
    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(config);
    driver.build();
    segmentDirectory = new File(INDEX_DIR, driver.getSegmentName());
  }

  @AfterMethod
  public void tearDown() {
    FileUtils.deleteQuietly(segmentDirectory);
  }

  @Test
  public void testToJson()
      throws IOException, ConfigurationException, JSONException {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDirectory);
    Assert.assertNotNull(metadata);
    JSONObject jsonMeta = metadata.toJson(null);
    Assert.assertEquals(jsonMeta.get("segmentName"), metadata.getName());
    Assert.assertEquals(jsonMeta.getLong("crc"), Long.valueOf(metadata.getCrc()).longValue());
    Assert.assertEquals(jsonMeta.getString("paddingCharacter"), String.valueOf(metadata.getPaddingCharacter()));
    Assert.assertEquals(jsonMeta.get("creatorName"), metadata.getCreatorName());
    Assert.assertEquals(jsonMeta.get("creationTimeMillis"), metadata.getIndexCreationTime());
    Assert.assertEquals(jsonMeta.get("startTimeMillis"), metadata.getTimeInterval().getStartMillis());
    Assert.assertEquals(jsonMeta.get("endTimeMillis"), metadata.getTimeInterval().getEndMillis());
    Assert.assertEquals(jsonMeta.get("pushTimeMillis"), metadata.getPushTime());
    Assert.assertEquals(jsonMeta.get("refreshTimeMillis"), metadata.getPushTime());
    JSONArray jsonColumnList = jsonMeta.getJSONArray("columns");
    Assert.assertEquals(jsonColumnList.length(), metadata.getAllColumns().size());
    for (int i = 0; i < jsonColumnList.length(); i++) {
      JSONObject  jsonColumn = jsonColumnList.getJSONObject(i);
      ColumnMetadata colMeta = metadata.getColumnMetadataFor(jsonColumn.getString("columnName"));
      Assert.assertEquals(jsonColumn.get("cardinality"), colMeta.getCardinality());
      Assert.assertEquals(jsonColumn.get("totalRawDocs"), colMeta.getTotalRawDocs());
      Assert.assertEquals(jsonColumn.get("bitsPerElement"), colMeta.getBitsPerElement());
      Assert.assertEquals(jsonColumn.getBoolean("sorted"), colMeta.isSorted());
      Assert.assertEquals(jsonColumn.get("totalAggDocs"), colMeta.getTotalAggDocs());
      Assert.assertEquals(jsonColumn.get("containsNulls"), colMeta.hasNulls());
      Assert.assertEquals(jsonColumn.getBoolean("hasDictionary"), colMeta.hasDictionary());
    }

  }
}
