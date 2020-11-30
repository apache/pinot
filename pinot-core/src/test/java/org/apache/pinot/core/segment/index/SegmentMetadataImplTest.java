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
package org.apache.pinot.core.segment.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SegmentMetadataImplTest {
  private static final String AVRO_DATA = "data/test_data-mv.avro";
  private static final File INDEX_DIR = new File(FileUtils.getTempDirectory(), "SegmentMetadataImplTest");
  private File segmentDirectory;

  @BeforeMethod
  public void setUp()
      throws Exception {
    final String filePath =
        TestUtils.getFileFromResourceUrl(SegmentMetadataImplTest.class.getClassLoader().getResource(AVRO_DATA));

    // intentionally changed this to TimeUnit.Hours to make it non-default for testing
    final SegmentGeneratorConfig config = SegmentTestUtils
        .getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch", TimeUnit.HOURS,
            "testTable");
    config.setSegmentNamePostfix("1");
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    config.setSkipTimeValueCheck(true);
    config.setCustomProperties(ImmutableMap.of("custom.k1", "v1", "custom.k2", "v2"));
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
      throws IOException {
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDirectory);
    assertNotNull(metadata);

    JsonNode jsonMeta = metadata.toJson(null);
    assertEquals(jsonMeta.get("segmentName").asText(), metadata.getName());
    assertEquals(jsonMeta.get("crc").asLong(), Long.valueOf(metadata.getCrc()).longValue());
    assertEquals(jsonMeta.get("paddingCharacter").asText(), String.valueOf(metadata.getPaddingCharacter()));
    assertTrue(jsonMeta.get("creatorName").isNull());
    assertEquals(jsonMeta.get("creationTimeMillis").asLong(), metadata.getIndexCreationTime());
    assertEquals(jsonMeta.get("startTimeMillis").asLong(), metadata.getTimeInterval().getStartMillis());
    assertEquals(jsonMeta.get("endTimeMillis").asLong(), metadata.getTimeInterval().getEndMillis());
    assertEquals(jsonMeta.get("pushTimeMillis").asLong(), metadata.getPushTime());
    assertEquals(jsonMeta.get("refreshTimeMillis").asLong(), metadata.getPushTime());
    assertEquals(jsonMeta.get("custom").get("k1").asText(), metadata.getCustomMap().get("k1"));
    assertEquals(jsonMeta.get("custom").get("k2").asText(), metadata.getCustomMap().get("k2"));

    JsonNode jsonColumnsMeta = jsonMeta.get("columns");
    int numColumns = jsonColumnsMeta.size();
    assertEquals(numColumns, metadata.getAllColumns().size());
    for (int i = 0; i < numColumns; i++) {
      JsonNode jsonColumnMeta = jsonColumnsMeta.get(i);
      ColumnMetadata columnMeta = metadata.getColumnMetadataFor(jsonColumnMeta.get("columnName").asText());
      assertNotNull(columnMeta);
      assertEquals(jsonColumnMeta.get("cardinality").asInt(), columnMeta.getCardinality());
      assertEquals(jsonColumnMeta.get("bitsPerElement").asInt(), columnMeta.getBitsPerElement());
      assertEquals(jsonColumnMeta.get("sorted").asBoolean(), columnMeta.isSorted());
      assertEquals(jsonColumnMeta.get("containsNulls").asBoolean(), columnMeta.hasNulls());
      assertEquals(jsonColumnMeta.get("hasDictionary").asBoolean(), columnMeta.hasDictionary());
    }
  }
}
