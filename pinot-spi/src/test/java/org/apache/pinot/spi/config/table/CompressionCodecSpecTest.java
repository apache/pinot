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
package org.apache.pinot.spi.config.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class CompressionCodecSpecTest {

  @Test
  public void testCanonicalizesAliasAndCase()
      throws JsonProcessingException {
    CompressionCodecSpec aliasSpec = CompressionCodecSpecParser.parse("zstd");
    assertEquals(aliasSpec.getCodec(), CompressionCodec.ZSTANDARD);
    assertEquals(aliasSpec.toConfigString(), "ZSTANDARD");

    CompressionCodecSpec leveledAliasSpec = CompressionCodecSpecParser.parse("zstd(3)");
    assertEquals(leveledAliasSpec.getCodec(), CompressionCodec.ZSTANDARD);
    assertEquals(leveledAliasSpec.getLevel(), Integer.valueOf(3));
    assertEquals(leveledAliasSpec.toConfigString(), "ZSTANDARD(3)");

    CompressionCodecSpec jsonSpec = JsonUtils.stringToObject("\"lz4(12)\"", CompressionCodecSpec.class);
    assertEquals(jsonSpec, CompressionCodecSpec.of(CompressionCodec.LZ4, 12));
    assertEquals(JsonUtils.objectToString(jsonSpec), "\"LZ4(12)\"");
  }

  @Test
  public void testSupportedLevelRanges() {
    assertEquals(CompressionCodecSpecParser.parse("gzip(0)"), CompressionCodecSpec.of(CompressionCodec.GZIP, 0));
    assertEquals(CompressionCodecSpecParser.parse("gzip(9)"), CompressionCodecSpec.of(CompressionCodec.GZIP, 9));
    assertEquals(CompressionCodecSpecParser.parse("lz4(17)"), CompressionCodecSpec.of(CompressionCodec.LZ4, 17));
    assertEquals(CompressionCodecSpecParser.parse("zstandard(22)"),
        CompressionCodecSpec.of(CompressionCodec.ZSTANDARD, 22));
  }

  @Test
  public void testSpecUtilitiesAndCapabilities() {
    assertNull(CompressionCodecSpec.fromCompressionCodec(null));

    CompressionCodecSpec snappySpec = CompressionCodecSpec.fromCompressionCodec(CompressionCodec.SNAPPY);
    assertEquals(snappySpec.toString(), "SNAPPY");
    assertFalse(snappySpec.hasLevel());
    assertNull(snappySpec.getLevel());

    assertTrue(CompressionCodecCapabilities.supportsLevel(CompressionCodec.ZSTANDARD));
    assertFalse(CompressionCodecCapabilities.supportsLevel(CompressionCodec.SNAPPY));
    assertEquals(CompressionCodecCapabilities.getMinLevel(CompressionCodec.GZIP), Integer.valueOf(0));
    assertEquals(CompressionCodecCapabilities.getMaxLevel(CompressionCodec.LZ4), Integer.valueOf(17));
    assertNull(CompressionCodecCapabilities.getMinLevel(CompressionCodec.PASS_THROUGH));
    assertNull(CompressionCodecCapabilities.getMaxLevel(CompressionCodec.PASS_THROUGH));
  }

  @Test
  public void testRejectsInvalidSpecs() {
    IllegalArgumentException nullSpec =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse(null));
    assertTrue(nullSpec.getMessage().contains("must not be null"));

    IllegalArgumentException emptySpec =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("   "));
    assertTrue(emptySpec.getMessage().contains("must not be empty"));

    IllegalArgumentException emptyLevel =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD()"));
    assertTrue(emptyLevel.getMessage().contains("non-empty integer"));

    IllegalArgumentException invalidLevel =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD(foo)"));
    assertTrue(invalidLevel.getMessage().contains("Expected an integer"));

    IllegalArgumentException unsupportedLevel =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("SNAPPY(3)"));
    assertTrue(unsupportedLevel.getMessage().contains("does not support a level parameter"));

    IllegalArgumentException unknownCodec =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("UNKNOWN(3)"));
    assertTrue(unknownCodec.getMessage().contains("Unknown compression codec"));

    IllegalArgumentException outOfRange =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("LZ4(18)"));
    assertTrue(outOfRange.getMessage().contains("out of range"));

    IllegalArgumentException malformed =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD(3))"));
    assertTrue(malformed.getMessage().contains("Expected CODEC or CODEC(level)"));

    IllegalArgumentException invalidCodecToken =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD-(3)"));
    assertTrue(invalidCodecToken.getMessage().contains("Expected CODEC or CODEC(level)"));

    IllegalArgumentException unknownAlias =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecCapabilities.canonicalizeAlias("unknown"));
    assertTrue(unknownAlias.getMessage().contains("Unknown compression codec 'UNKNOWN'"));
  }
}
