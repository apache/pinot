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
package org.apache.pinot.common.restlet.resources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SegmentSizeInfoTest {

  @Test
  public void testJsonRoundTripWithCompressionStats()
      throws Exception {
    Map<String, ColumnCompressionStatsInfo> columnStats = new HashMap<>();
    columnStats.put("col1", new ColumnCompressionStatsInfo("col1", 10000, 2500, 4.0, "LZ4",
        List.of("forward_index"), null));
    columnStats.put("col2", new ColumnCompressionStatsInfo("col2", 20000, 4000, 5.0, "ZSTANDARD",
        List.of("forward_index"), null));

    SegmentSizeInfo original = new SegmentSizeInfo("seg1", 50000, 30000, 6500, "tier1", columnStats);

    String json = JsonUtils.objectToString(original);
    SegmentSizeInfo deserialized = JsonUtils.stringToObject(json, SegmentSizeInfo.class);

    assertEquals(deserialized.getSegmentName(), "seg1");
    assertEquals(deserialized.getDiskSizeInBytes(), 50000);
    assertEquals(deserialized.getRawIngestSizeBytes(), 30000);
    assertEquals(deserialized.getOnDiskSizeBytes(), 6500);
    assertEquals(deserialized.getTier(), "tier1");
    assertNotNull(deserialized.getColumnCompressionStats());
    assertEquals(deserialized.getColumnCompressionStats().size(), 2);

    ColumnCompressionStatsInfo col1Stats = deserialized.getColumnCompressionStats().get("col1");
    assertNotNull(col1Stats);
    assertEquals(col1Stats.getColumn(), "col1");
    assertEquals(col1Stats.getRawIngestSizeInBytes(), 10000);
    assertEquals(col1Stats.getOnDiskSizeInBytes(), 2500);
    assertEquals(col1Stats.getCompressionRatio(), 4.0, 0.01);
    assertEquals(col1Stats.getCodec(), "LZ4");
  }

  @Test
  public void testJsonRoundTripBackwardCompatible()
      throws Exception {
    // Simulate old server response without compression fields
    String oldJson = "{\"segmentName\":\"seg1\",\"diskSizeInBytes\":50000}";
    SegmentSizeInfo deserialized = JsonUtils.stringToObject(oldJson, SegmentSizeInfo.class);

    assertEquals(deserialized.getSegmentName(), "seg1");
    assertEquals(deserialized.getDiskSizeInBytes(), 50000);
    assertEquals(deserialized.getRawIngestSizeBytes(), 0);
    assertEquals(deserialized.getOnDiskSizeBytes(), 0);
    assertNull(deserialized.getTier());
    assertNull(deserialized.getColumnCompressionStats());
  }

  @Test
  public void testJsonRoundTripWithoutColumnStats()
      throws Exception {
    SegmentSizeInfo original = new SegmentSizeInfo("seg1", 50000, 30000, 6500, "default");

    String json = JsonUtils.objectToString(original);
    SegmentSizeInfo deserialized = JsonUtils.stringToObject(json, SegmentSizeInfo.class);

    assertEquals(deserialized.getSegmentName(), "seg1");
    assertEquals(deserialized.getDiskSizeInBytes(), 50000);
    assertEquals(deserialized.getRawIngestSizeBytes(), 30000);
    assertEquals(deserialized.getOnDiskSizeBytes(), 6500);
    assertEquals(deserialized.getTier(), "default");
    assertNull(deserialized.getColumnCompressionStats());
  }

  @Test
  public void testLegacyTwoArgConstructor() {
    SegmentSizeInfo info = new SegmentSizeInfo("seg1", 1000);
    assertEquals(info.getSegmentName(), "seg1");
    assertEquals(info.getDiskSizeInBytes(), 1000);
    assertEquals(info.getRawIngestSizeBytes(), -1);
    assertEquals(info.getOnDiskSizeBytes(), -1);
    assertNull(info.getTier());
    assertNull(info.getColumnCompressionStats());
  }
}
