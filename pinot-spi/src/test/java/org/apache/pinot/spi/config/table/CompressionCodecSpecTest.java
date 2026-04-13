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
import java.util.Collections;
import java.util.Map;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class CompressionCodecSpecTest {

  @Test
  public void testParsesAndCanonicalizesAliasAndCase()
      throws JsonProcessingException {
    CompressionCodecSpec aliasSpec = CompressionCodecSpecParser.parse(" zstd ");
    assertEquals(aliasSpec.getName(), "ZSTANDARD");
    assertTrue(aliasSpec.getParams().isEmpty());
    assertFalse(aliasSpec.hasLevel());
    assertNull(aliasSpec.getLevel());
    assertEquals(aliasSpec.toConfigString(), "ZSTANDARD");
    assertSame(aliasSpec.getCodec(), CompressionCodec.ZSTANDARD);

    CompressionCodecSpec leveledAliasSpec = CompressionCodecSpecParser.parse("lz4hc(5)");
    assertEquals(leveledAliasSpec.getName(), "LZ4HC");
    assertEquals(leveledAliasSpec.getParams(), Map.of("level", "5"));
    assertEquals(leveledAliasSpec.getLevel(), Integer.valueOf(5));
    assertEquals(leveledAliasSpec.toConfigString(), "LZ4HC(5)");
    assertNull(leveledAliasSpec.getCodec());

    CompressionCodecSpec jsonSpec = JsonUtils.stringToObject("\"zstd(3)\"", CompressionCodecSpec.class);
    assertEquals(jsonSpec, CompressionCodecSpecParser.parse("ZSTANDARD(3)"));
    assertEquals(JsonUtils.objectToString(jsonSpec), "\"ZSTANDARD(3)\"");
  }

  @Test
  public void testGenericFactoryPreservesParamsAndEquality() {
    CompressionCodecSpec spec = CompressionCodecSpec.of("zstandard", Collections.singletonMap("level", "3"));
    assertEquals(spec.getName(), "ZSTANDARD");
    assertEquals(spec.getParams(), Map.of("level", "3"));
    assertEquals(spec, CompressionCodecSpecParser.parse("ZSTD(3)"));
    assertEquals(spec.hashCode(), CompressionCodecSpecParser.parse("zstd(3)").hashCode());
    assertEquals(spec.toString(), "ZSTANDARD(3)");
  }

  @Test
  public void testCompatibilityFactories() {
    assertNull(CompressionCodecSpec.fromCompressionCodec(null));

    CompressionCodecSpec snappySpec = CompressionCodecSpec.fromCompressionCodec(CompressionCodec.SNAPPY);
    assertEquals(snappySpec.toString(), "SNAPPY");
    assertSame(snappySpec.getCodec(), CompressionCodec.SNAPPY);

    CompressionCodecSpec leveledSpec = CompressionCodecSpec.of(CompressionCodec.GZIP, 6);
    assertEquals(leveledSpec.getName(), "GZIP");
    assertEquals(leveledSpec.getLevel(), Integer.valueOf(6));
    assertEquals(leveledSpec.toConfigString(), "GZIP(6)");
    assertSame(leveledSpec.getCodec(), CompressionCodec.GZIP);
  }

  @Test
  public void testParamsAreImmutable() {
    CompressionCodecSpec spec = CompressionCodecSpecParser.parse("ZSTD(3)");
    expectThrows(UnsupportedOperationException.class, () -> spec.getParams().put("foo", "bar"));
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

    IllegalArgumentException multiLevel =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD(1,2)"));
    assertTrue(multiLevel.getMessage().contains("Expected an integer"));

    IllegalArgumentException malformed =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD(3))"));
    assertTrue(malformed.getMessage().contains("Expected CODEC or CODEC(level)"));

    IllegalArgumentException invalidCodecToken =
        expectThrows(IllegalArgumentException.class, () -> CompressionCodecSpecParser.parse("ZSTD-(3)"));
    assertTrue(invalidCodecToken.getMessage().contains("Expected CODEC or CODEC(level)"));

    CompressionCodecSpec pluginFriendlySpec = CompressionCodecSpecParser.parse("unknown(7)");
    assertEquals(pluginFriendlySpec.getName(), "UNKNOWN");
    assertEquals(pluginFriendlySpec.getLevel(), Integer.valueOf(7));
    assertNull(pluginFriendlySpec.getCodec());
  }
}
