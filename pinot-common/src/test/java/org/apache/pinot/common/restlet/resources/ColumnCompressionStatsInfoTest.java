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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ColumnCompressionStatsInfoTest {

  @Test
  public void testGetters() {
    List<String> indexes = Arrays.asList("forward_index", "inverted_index");
    ColumnCompressionStatsInfo info =
        new ColumnCompressionStatsInfo("myCol", 8000L, 2000L, 4.0, "LZ4", indexes, null);

    assertEquals(info.getColumn(), "myCol");
    assertEquals(info.getRawIngestSizeInBytes(), 8000L);
    assertEquals(info.getOnDiskSizeInBytes(), 2000L);
    assertEquals(info.getCompressionRatio(), 4.0, 1e-9);
    assertEquals(info.getCodec(), "LZ4");
    assertNull(info.getCodecBreakdown());
    assertEquals(info.getIndexes(), indexes);
  }

  @Test
  public void testDictEncodedCodec() {
    ColumnCompressionStatsInfo info =
        new ColumnCompressionStatsInfo("dictCol", -1L, 1000L, 0.0,
            ColumnCompressionStatsInfo.CODEC_DICT_ENCODED, List.of("forward_index"), null);

    assertEquals(info.getCodec(), ColumnCompressionStatsInfo.CODEC_DICT_ENCODED);
    assertEquals(info.getRawIngestSizeInBytes(), -1L);
  }

  @Test
  public void testJsonRoundTrip()
      throws Exception {
    List<String> indexes = Arrays.asList("forward_index", "range_index");
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("col1", 10000L, 2500L, 4.0, "ZSTANDARD", indexes, null);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "col1");
    assertEquals(deserialized.getRawIngestSizeInBytes(), 10000L);
    assertEquals(deserialized.getOnDiskSizeInBytes(), 2500L);
    assertEquals(deserialized.getCompressionRatio(), 4.0, 1e-9);
    assertEquals(deserialized.getCodec(), "ZSTANDARD");
    assertNull(deserialized.getCodecBreakdown());
    assertNotNull(deserialized.getIndexes());
    assertEquals(deserialized.getIndexes().size(), 2);
    assertTrue(deserialized.getIndexes().contains("forward_index"));
    assertTrue(deserialized.getIndexes().contains("range_index"));
  }

  @Test
  public void testNullCodecAndNullIndexesRoundTrip()
      throws Exception {
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("noCodecCol", 3000L, 1500L, 2.0, null, null, null);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "noCodecCol");
    assertEquals(deserialized.getRawIngestSizeInBytes(), 3000L);
    assertEquals(deserialized.getOnDiskSizeInBytes(), 1500L);
    assertEquals(deserialized.getCompressionRatio(), 2.0, 1e-9);
    assertNull(deserialized.getCodec());
    assertNull(deserialized.getIndexes());
    assertNull(deserialized.getCodecBreakdown());
  }

  @Test
  public void testJsonIgnoresUnknownAndLegacyFields()
      throws Exception {
    // hasDictionary is a legacy field — it should be silently ignored (JsonIgnoreProperties)
    String json = "{\"column\":\"futureCol\",\"rawIngestSizeInBytes\":6000,"
        + "\"onDiskSizeInBytes\":1200,\"compressionRatio\":5.0,"
        + "\"codec\":\"LZ4\",\"hasDictionary\":false,"
        + "\"indexes\":[\"forward_index\"],\"unknownField\":\"ignored\"}";

    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "futureCol");
    assertEquals(deserialized.getRawIngestSizeInBytes(), 6000L);
    assertEquals(deserialized.getOnDiskSizeInBytes(), 1200L);
    assertEquals(deserialized.getCompressionRatio(), 5.0, 1e-9);
    assertEquals(deserialized.getCodec(), "LZ4");
    assertNotNull(deserialized.getIndexes());
    assertEquals(deserialized.getIndexes(), List.of("forward_index"));
    assertNull(deserialized.getCodecBreakdown());
  }

  @Test
  public void testDictEncodedCodecJsonRoundTrip()
      throws Exception {
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("dictRoundTrip", -1L, 3500L, 0.0,
            ColumnCompressionStatsInfo.CODEC_DICT_ENCODED, List.of("forward_index"), null);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "dictRoundTrip");
    assertEquals(deserialized.getCodec(), ColumnCompressionStatsInfo.CODEC_DICT_ENCODED);
    assertEquals(deserialized.getRawIngestSizeInBytes(), -1L);
    assertNull(deserialized.getCodecBreakdown());
  }

  @Test
  public void testCodecBreakdownRoundTrip()
      throws Exception {
    Map<String, ColumnCompressionStatsInfo.CodecBreakdownEntry> breakdown = Map.of(
        "LZ4", new ColumnCompressionStatsInfo.CodecBreakdownEntry(3, 9000L, 2000L),
        "DICT_ENCODED", new ColumnCompressionStatsInfo.CodecBreakdownEntry(2, 0L, 1500L));
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("mixedCol", 9000L, 3500L, 2.57, "MIXED",
            List.of("forward_index"), breakdown);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getCodec(), "MIXED");
    assertNotNull(deserialized.getCodecBreakdown());
    assertEquals(deserialized.getCodecBreakdown().size(), 2);

    ColumnCompressionStatsInfo.CodecBreakdownEntry lz4 = deserialized.getCodecBreakdown().get("LZ4");
    assertNotNull(lz4);
    assertEquals(lz4.getSegments(), 3);
    assertEquals(lz4.getRawIngestSizeInBytes(), 9000L);
    assertEquals(lz4.getOnDiskSizeInBytes(), 2000L);

    ColumnCompressionStatsInfo.CodecBreakdownEntry dict =
        deserialized.getCodecBreakdown().get("DICT_ENCODED");
    assertNotNull(dict);
    assertEquals(dict.getSegments(), 2);
    assertEquals(dict.getRawIngestSizeInBytes(), 0L);
    assertEquals(dict.getOnDiskSizeInBytes(), 1500L);
  }
}
