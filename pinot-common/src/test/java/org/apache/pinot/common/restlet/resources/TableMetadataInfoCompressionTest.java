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
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests the TableMetadataInfo response schema for compression stats (T056/T057).
 * Validates server-side response includes columnCompressionStats when present
 * and suppresses it (via NON_NULL) when absent.
 */
public class TableMetadataInfoCompressionTest {

  @Test
  public void testSerializationWithCompressionStats()
      throws Exception {
    Map<String, ColumnCompressionStatsInfo> colStats = new HashMap<>();
    colStats.put("col_a", new ColumnCompressionStatsInfo(10000, 2000, "LZ4"));
    colStats.put("col_b", new ColumnCompressionStatsInfo(20000, 5000, "ZSTANDARD"));

    TableMetadataInfo info = new TableMetadataInfo("testTable", 50000, 3, 1000,
        Map.of("col_a", 4.0), Map.of("col_a", 50.0), Map.of(), Map.of(), Map.of(), colStats);

    String json = JsonUtils.objectToString(info);
    JsonNode node = JsonUtils.stringToJsonNode(json);

    // columnCompressionStats should be present
    assertTrue(node.has("columnCompressionStats"));
    JsonNode colStatsNode = node.get("columnCompressionStats");
    assertTrue(colStatsNode.has("col_a"));
    assertTrue(colStatsNode.has("col_b"));

    // Validate col_a values
    JsonNode colA = colStatsNode.get("col_a");
    assertEquals(colA.get("rawForwardIndexSizeBytes").asLong(), 10000);
    assertEquals(colA.get("compressedForwardIndexSizeBytes").asLong(), 2000);
    assertEquals(colA.get("compressionCodec").asText(), "LZ4");

    // Validate col_b values
    JsonNode colB = colStatsNode.get("col_b");
    assertEquals(colB.get("rawForwardIndexSizeBytes").asLong(), 20000);
    assertEquals(colB.get("compressedForwardIndexSizeBytes").asLong(), 5000);
    assertEquals(colB.get("compressionCodec").asText(), "ZSTANDARD");
  }

  @Test
  public void testSerializationWithoutCompressionStats()
      throws Exception {
    // Use backwards-compatible constructor (no compression stats)
    TableMetadataInfo info = new TableMetadataInfo("testTable", 50000, 3, 1000,
        Map.of("col_a", 4.0), Map.of("col_a", 50.0), Map.of(), Map.of(), Map.of());

    String json = JsonUtils.objectToString(info);
    JsonNode node = JsonUtils.stringToJsonNode(json);

    // columnCompressionStats should be absent (suppressed by NON_NULL)
    assertFalse(node.has("columnCompressionStats"),
        "columnCompressionStats should be suppressed from JSON when null");
  }

  @Test
  public void testDeserializationRoundTrip()
      throws Exception {
    Map<String, ColumnCompressionStatsInfo> colStats = new HashMap<>();
    colStats.put("metric_col", new ColumnCompressionStatsInfo(50000, 8000, "SNAPPY"));

    TableMetadataInfo original = new TableMetadataInfo("roundTripTable", 100000, 5, 5000,
        Map.of("metric_col", 8.0), Map.of("metric_col", 100.0), Map.of(), Map.of(), Map.of(), colStats);

    String json = JsonUtils.objectToString(original);
    TableMetadataInfo deserialized = JsonUtils.stringToObject(json, TableMetadataInfo.class);

    assertEquals(deserialized.getTableName(), "roundTripTable");
    assertEquals(deserialized.getDiskSizeInBytes(), 100000);
    assertNotNull(deserialized.getColumnCompressionStats());
    assertEquals(deserialized.getColumnCompressionStats().size(), 1);

    ColumnCompressionStatsInfo stats = deserialized.getColumnCompressionStats().get("metric_col");
    assertNotNull(stats);
    assertEquals(stats.getRawForwardIndexSizeBytes(), 50000);
    assertEquals(stats.getCompressedForwardIndexSizeBytes(), 8000);
    assertEquals(stats.getCompressionCodec(), "SNAPPY");
  }

  @Test
  public void testBackwardCompatDeserialization()
      throws Exception {
    // Simulate JSON from an old server that doesn't include columnCompressionStats
    String oldJson = "{\"tableName\":\"oldTable\",\"diskSizeInBytes\":30000,"
        + "\"numSegments\":2,\"numRows\":500,"
        + "\"columnLengthMap\":{\"col\":4.0},"
        + "\"columnCardinalityMap\":{\"col\":10.0},"
        + "\"maxNumMultiValuesMap\":{},"
        + "\"columnIndexSizeMap\":{},"
        + "\"upsertPartitionToServerPrimaryKeyCountMap\":{}}";

    TableMetadataInfo info = JsonUtils.stringToObject(oldJson, TableMetadataInfo.class);
    assertNotNull(info);
    assertEquals(info.getTableName(), "oldTable");
    assertEquals(info.getDiskSizeInBytes(), 30000);
    // columnCompressionStats should be null (not present in old JSON)
    assertNull(info.getColumnCompressionStats());
  }
}
