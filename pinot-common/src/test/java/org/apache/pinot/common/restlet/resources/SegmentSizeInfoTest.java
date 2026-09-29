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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies legacy and compression-aware JSON forms of [SegmentSizeInfo].
public class SegmentSizeInfoTest {
  @Test
  public void testJsonRoundTripWithCompressionStats()
      throws Exception {
    ColumnCompressionStatsInfo columnInfo = ColumnCompressionStatsInfo.builder("col1")
        .withUncompressedValueSizeInBytes(10_000)
        .withForwardIndexAndDictionaryStorageSizeInBytes(2_500)
        .withCompressionRatio(4.0)
        .withObservedIndexes(List.of("forward_index"))
        .withEncodingBreakdown(List.of(new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.RAW, ChunkCompressionType.LZ4, 1, 10_000, 2_500)))
        .withNumSegments(1)
        .build();
    SegmentSizeInfo original =
        new SegmentSizeInfo("seg1", 50_000, 10_000, 2_500, Map.of("col1", columnInfo));

    String serialized = JsonUtils.objectToString(original);
    JsonNode json = JsonUtils.stringToJsonNode(serialized);
    assertTrue(json.has("compressionStatsUncompressedValueSizeInBytes"));
    assertTrue(json.has("compressionStatsForwardIndexAndDictionaryStorageSizeInBytes"));
    assertFalse(json.has("uncompressedValueSizeBytes"));
    assertFalse(json.has("forwardIndexStorageSizeBytes"));
    assertEquals(json.get("columnCompressionStats").get("col1").get("compressionRatio").asDouble(), 4.0);

    SegmentSizeInfo deserialized = JsonUtils.stringToObject(serialized, SegmentSizeInfo.class);
    assertEquals(deserialized.getCompressionStatsUncompressedValueSizeInBytes(), 10_000L);
    assertEquals(deserialized.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), 2_500L);
    assertNotNull(deserialized.getColumnCompressionStats());
  }

  @Test
  public void testUnreleasedAliasesAreIgnored()
      throws Exception {
    String json = "{\"segmentName\":\"seg1\",\"diskSizeInBytes\":50000,"
        + "\"uncompressedValueSizeBytes\":1200,\"forwardIndexStorageSizeBytes\":300}";
    SegmentSizeInfo deserialized = JsonUtils.stringToObject(json, SegmentSizeInfo.class);
    assertEquals(deserialized.getCompressionStatsUncompressedValueSizeInBytes(), -1L);
    assertEquals(deserialized.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), -1L);
  }

  @Test
  public void testLegacyTwoArgConstructorEmitsOnlyLegacyFields()
      throws Exception {
    SegmentSizeInfo info = new SegmentSizeInfo("seg1", 1_000);
    JsonNode json = JsonUtils.stringToJsonNode(JsonUtils.objectToString(info));

    assertEquals(json.size(), 2);
    assertEquals(json.get("segmentName").asText(), "seg1");
    assertEquals(json.get("diskSizeInBytes").asLong(), 1_000L);
    assertEquals(info.getCompressionStatsUncompressedValueSizeInBytes(), -1L);
    assertEquals(info.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), -1L);
    assertNull(info.getColumnCompressionStats());
  }

  @Test
  public void testZeroSizesRemainAvailable()
      throws Exception {
    SegmentSizeInfo original = new SegmentSizeInfo("empty", 0, 0, 0);
    String serialized = JsonUtils.objectToString(original);
    SegmentSizeInfo deserialized = JsonUtils.stringToObject(serialized, SegmentSizeInfo.class);

    assertEquals(deserialized.getCompressionStatsUncompressedValueSizeInBytes(), 0L);
    assertEquals(deserialized.getCompressionStatsForwardIndexAndDictionaryStorageSizeInBytes(), 0L);
  }
}
