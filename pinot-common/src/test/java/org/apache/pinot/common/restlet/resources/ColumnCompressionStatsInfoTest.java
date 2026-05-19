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
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ColumnCompressionStatsInfoTest {

  @Test
  public void testGetters() {
    List<String> indexes = Arrays.asList("forward_index", "inverted_index");
    ColumnCompressionStatsInfo info =
        new ColumnCompressionStatsInfo("myCol", 8000L, 2000L, 4.0, "LZ4", false, indexes);

    assertEquals(info.getColumn(), "myCol");
    assertEquals(info.getUncompressedSizeInBytes(), 8000L);
    assertEquals(info.getCompressedSizeInBytes(), 2000L);
    assertEquals(info.getCompressionRatio(), 4.0, 1e-9);
    assertEquals(info.getCodec(), "LZ4");
    assertFalse(info.hasDictionary());
    assertEquals(info.getIndexes(), indexes);
  }

  @Test
  public void testHasDictionaryTrue() {
    ColumnCompressionStatsInfo info =
        new ColumnCompressionStatsInfo("dictCol", 5000L, 1000L, 5.0, "SNAPPY", true,
            List.of("forward_index"));

    assertTrue(info.hasDictionary());
    assertEquals(info.getCodec(), "SNAPPY");
  }

  @Test
  public void testJsonRoundTrip()
      throws Exception {
    List<String> indexes = Arrays.asList("forward_index", "range_index");
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("col1", 10000L, 2500L, 4.0, "ZSTANDARD", false, indexes);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "col1");
    assertEquals(deserialized.getUncompressedSizeInBytes(), 10000L);
    assertEquals(deserialized.getCompressedSizeInBytes(), 2500L);
    assertEquals(deserialized.getCompressionRatio(), 4.0, 1e-9);
    assertEquals(deserialized.getCodec(), "ZSTANDARD");
    assertFalse(deserialized.hasDictionary());
    assertNotNull(deserialized.getIndexes());
    assertEquals(deserialized.getIndexes().size(), 2);
    assertTrue(deserialized.getIndexes().contains("forward_index"));
    assertTrue(deserialized.getIndexes().contains("range_index"));
  }

  @Test
  public void testNullCodecAndNullIndexesRoundTrip()
      throws Exception {
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("noCodecCol", 3000L, 1500L, 2.0, null, false, null);

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "noCodecCol");
    assertEquals(deserialized.getUncompressedSizeInBytes(), 3000L);
    assertEquals(deserialized.getCompressedSizeInBytes(), 1500L);
    assertEquals(deserialized.getCompressionRatio(), 2.0, 1e-9);
    assertNull(deserialized.getCodec());
    assertFalse(deserialized.hasDictionary());
    assertNull(deserialized.getIndexes());
  }

  @Test
  public void testJsonIgnoresUnknownFields()
      throws Exception {
    String json = "{\"column\":\"futureCol\",\"uncompressedSizeInBytes\":6000,"
        + "\"compressedSizeInBytes\":1200,\"compressionRatio\":5.0,"
        + "\"codec\":\"LZ4\",\"hasDictionary\":false,"
        + "\"indexes\":[\"forward_index\"],\"unknownField\":\"ignored\"}";

    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "futureCol");
    assertEquals(deserialized.getUncompressedSizeInBytes(), 6000L);
    assertEquals(deserialized.getCompressedSizeInBytes(), 1200L);
    assertEquals(deserialized.getCompressionRatio(), 5.0, 1e-9);
    assertEquals(deserialized.getCodec(), "LZ4");
    assertFalse(deserialized.hasDictionary());
    assertNotNull(deserialized.getIndexes());
    assertEquals(deserialized.getIndexes(), List.of("forward_index"));
  }

  @Test
  public void testHasDictionaryJsonRoundTrip()
      throws Exception {
    ColumnCompressionStatsInfo original =
        new ColumnCompressionStatsInfo("dictRoundTrip", 7000L, 3500L, 2.0, null, true,
            List.of("forward_index"));

    String json = JsonUtils.objectToString(original);
    ColumnCompressionStatsInfo deserialized =
        JsonUtils.stringToObject(json, ColumnCompressionStatsInfo.class);

    assertEquals(deserialized.getColumn(), "dictRoundTrip");
    assertTrue(deserialized.hasDictionary());
    assertNull(deserialized.getCodec());
  }
}
