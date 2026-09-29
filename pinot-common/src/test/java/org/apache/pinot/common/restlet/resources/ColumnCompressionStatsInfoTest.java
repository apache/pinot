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
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// Verifies JSON compatibility and immutability for [ColumnCompressionStatsInfo].
public class ColumnCompressionStatsInfoTest {
  @Test
  public void testRawEncodingJsonRoundTrip()
      throws Exception {
    ColumnCompressionStatsInfo original = info("col1", 10_000, 2_500, 4.0,
        List.of("forward_index", "range_index"),
        List.of(new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.RAW, ChunkCompressionType.ZSTANDARD, 3, 10_000, 2_500)), 3);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized = JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "col1");
    assertEquals(deserialized.getUncompressedValueSizeInBytes(), 10_000L);
    assertEquals(deserialized.getForwardIndexAndDictionaryStorageSizeInBytes(), 2_500L);
    assertEquals(deserialized.getCompressionRatio(), 4.0, 1e-9);
    assertEquals(deserialized.getObservedIndexes(), List.of("forward_index", "range_index"));
    assertEquals(deserialized.getNumSegments(), 3);
    assertEquals(deserialized.getEncodingBreakdown().size(), 1);
    ColumnCompressionStatsInfo.EncodingBreakdownEntry entry = deserialized.getEncodingBreakdown().get(0);
    assertEquals(entry.getEncoding(), EncodingType.RAW);
    assertEquals(entry.getChunkCompressionType(), ChunkCompressionType.ZSTANDARD);
    assertEquals(entry.getNumSegments(), 3);
  }

  @Test
  public void testDictionaryEncodingHasNoChunkCompressionType()
      throws Exception {
    ColumnCompressionStatsInfo original = info("dictCol", 4_000, 1_000, 4.0,
        List.of("dictionary", "forward_index"),
        List.of(new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.DICTIONARY, null, 2, 4_000, 1_000)), 2);

    JsonNode json = JsonUtils.stringToJsonNode(JsonUtils.objectToString(original));
    JsonNode entry = json.get("encodingBreakdown").get(0);
    assertEquals(entry.get("encoding").asText(), "DICTIONARY");
    assertFalse(entry.has("chunkCompressionType"));

    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json.toString(), ColumnCompressionStatsInfo.class);
    assertNull(deserialized.getEncodingBreakdown().get(0).getChunkCompressionType());
  }

  @Test
  public void testMixedEncodingRoundTrip()
      throws Exception {
    List<ColumnCompressionStatsInfo.EncodingBreakdownEntry> breakdown = List.of(
        new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.RAW, ChunkCompressionType.LZ4, 3, 9_000, 2_000),
        new ColumnCompressionStatsInfo.EncodingBreakdownEntry(EncodingType.DICTIONARY, null, 2, 1_000, 1_500));
    ColumnCompressionStatsInfo original = info(
        "mixedCol", 10_000, 3_500, 10_000d / 3_500, List.of("forward_index", "dictionary"), breakdown, 5);

    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(JsonUtils.objectToString(original), ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getEncodingBreakdown().size(), 2);
    assertEquals(deserialized.getEncodingBreakdown().get(0).getEncoding(), EncodingType.RAW);
    assertEquals(deserialized.getEncodingBreakdown().get(1).getEncoding(), EncodingType.DICTIONARY);
    assertEquals(deserialized.getNumSegments(), 5);
  }

  @Test
  public void testNullIndexesAreOmitted()
      throws Exception {
    ColumnCompressionStatsInfo info = info("col", 0, 0, 0, null,
        List.of(new ColumnCompressionStatsInfo.EncodingBreakdownEntry(
            EncodingType.RAW, ChunkCompressionType.PASS_THROUGH, 1, 0, 0)),
        1);

    JsonNode json = JsonUtils.stringToJsonNode(JsonUtils.objectToString(info));
    assertFalse(json.has("observedIndexes"));
    assertTrue(json.has("encodingBreakdown"));
  }

  @Test
  public void testUnknownAndLegacyFieldsAreIgnored()
      throws Exception {
    String json = "{\"column\":\"futureCol\",\"uncompressedValueSizeInBytes\":6000,"
        + "\"forwardIndexAndDictionaryStorageSizeInBytes\":1200,\"compressionRatio\":5.0,\"codec\":\"LZ4\","
        + "\"hasDictionary\":false,\"unknownField\":\"ignored\"}";

    ColumnCompressionStatsInfo deserialized = JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);
    assertEquals(deserialized.getColumn(), "futureCol");
    assertEquals(deserialized.getUncompressedValueSizeInBytes(), 6_000L);
    assertTrue(deserialized.getEncodingBreakdown().isEmpty());
  }

  private static ColumnCompressionStatsInfo info(String column, long uncompressedValueSizeInBytes,
      long forwardIndexAndDictionaryStorageSizeInBytes, double compressionRatio, List<String> observedIndexes,
      List<ColumnCompressionStatsInfo.EncodingBreakdownEntry> encodingBreakdown, int numSegments) {
    return ColumnCompressionStatsInfo.builder(column)
        .withUncompressedValueSizeInBytes(uncompressedValueSizeInBytes)
        .withForwardIndexAndDictionaryStorageSizeInBytes(forwardIndexAndDictionaryStorageSizeInBytes)
        .withCompressionRatio(compressionRatio)
        .withObservedIndexes(observedIndexes)
        .withEncodingBreakdown(encodingBreakdown)
        .withNumSegments(numSegments)
        .build();
  }
}
