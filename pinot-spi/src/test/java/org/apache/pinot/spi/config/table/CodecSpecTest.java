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
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalInt;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class CodecSpecTest {

  // ---- Valid parse tests ----

  @Test
  public void testParseZstdAlias() {
    CodecSpec spec = CodecSpecParser.parse("ZSTD");
    Assert.assertEquals(spec.getName(), "ZSTANDARD");
    Assert.assertTrue(spec.getParams().isEmpty());
    Assert.assertEquals(spec.getLevel(), OptionalInt.empty());
  }

  @Test
  public void testParseZstdWithLevel() {
    CodecSpec spec = CodecSpecParser.parse("ZSTD(3)");
    Assert.assertEquals(spec.getName(), "ZSTANDARD");
    Assert.assertEquals(spec.getLevel(), OptionalInt.of(3));
    Assert.assertEquals(spec.getParams().get("level"), "3");
  }

  @Test
  public void testParseLowercaseZstdWithLevel() {
    CodecSpec spec = CodecSpecParser.parse("zstd(3)");
    Assert.assertEquals(spec.getName(), "ZSTANDARD");
    Assert.assertEquals(spec.getLevel(), OptionalInt.of(3));
  }

  @Test
  public void testParseLz4hcWithLevel() {
    CodecSpec spec = CodecSpecParser.parse("LZ4HC(5)");
    Assert.assertEquals(spec.getName(), "LZ4HC");
    Assert.assertEquals(spec.getLevel(), OptionalInt.of(5));
  }

  @Test
  public void testParseGzip() {
    CodecSpec spec = CodecSpecParser.parse("GZIP");
    Assert.assertEquals(spec.getName(), "GZIP");
    Assert.assertTrue(spec.getParams().isEmpty());
  }

  @Test
  public void testParseSnappy() {
    CodecSpec spec = CodecSpecParser.parse("SNAPPY");
    Assert.assertEquals(spec.getName(), "SNAPPY");
    Assert.assertTrue(spec.getParams().isEmpty());
  }

  @Test
  public void testParsePassThrough() {
    CodecSpec spec = CodecSpecParser.parse("PASS_THROUGH");
    Assert.assertEquals(spec.getName(), "PASS_THROUGH");
    Assert.assertTrue(spec.getParams().isEmpty());
  }

  @Test
  public void testParseWithWhitespace() {
    CodecSpec spec = CodecSpecParser.parse("  ZSTD(3)  ");
    Assert.assertEquals(spec.getName(), "ZSTANDARD");
    Assert.assertEquals(spec.getLevel(), OptionalInt.of(3));
  }

  // ---- Invalid parse tests ----

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNull() {
    CodecSpecParser.parse(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmpty() {
    CodecSpecParser.parse("");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseBlank() {
    CodecSpecParser.parse("   ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmptyParens() {
    CodecSpecParser.parse("ZSTD()");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNonIntegerArg() {
    CodecSpecParser.parse("ZSTD(a)");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseMultipleArgs() {
    CodecSpecParser.parse("ZSTD(1,2)");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseUnbalancedOpenParen() {
    CodecSpecParser.parse("ZSTD(3");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseUnbalancedCloseParen() {
    CodecSpecParser.parse("ZSTD)3");
  }

  // ---- toString tests ----

  @Test
  public void testToStringSimple() {
    CodecSpec spec = CodecSpecParser.parse("SNAPPY");
    Assert.assertEquals(spec.toString(), "SNAPPY");
  }

  @Test
  public void testToStringWithLevel() {
    CodecSpec spec = CodecSpecParser.parse("ZSTD(3)");
    Assert.assertEquals(spec.toString(), "ZSTANDARD(3)");
  }

  // ---- toString round-trip tests ----

  @Test
  public void testToStringRoundTripSimple() {
    CodecSpec original = CodecSpecParser.parse("SNAPPY");
    CodecSpec reparsed = CodecSpecParser.parse(original.toString());
    Assert.assertEquals(reparsed, original);
  }

  @Test
  public void testToStringRoundTripWithLevel() {
    CodecSpec original = CodecSpecParser.parse("ZSTD(3)");
    CodecSpec reparsed = CodecSpecParser.parse(original.toString());
    Assert.assertEquals(reparsed, original);
  }

  @Test
  public void testToStringRoundTripLz4hc() {
    CodecSpec original = CodecSpecParser.parse("LZ4HC(5)");
    CodecSpec reparsed = CodecSpecParser.parse(original.toString());
    Assert.assertEquals(reparsed, original);
  }

  // ---- equals and hashCode tests ----

  @Test
  public void testEqualsIdentical() {
    CodecSpec a = CodecSpecParser.parse("ZSTD(3)");
    CodecSpec b = CodecSpecParser.parse("zstd(3)");
    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testEqualsSameNameNoParams() {
    CodecSpec a = CodecSpecParser.parse("SNAPPY");
    CodecSpec b = CodecSpecParser.parse("snappy");
    Assert.assertEquals(a, b);
    Assert.assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testNotEqualsDifferentName() {
    CodecSpec a = CodecSpecParser.parse("SNAPPY");
    CodecSpec b = CodecSpecParser.parse("GZIP");
    Assert.assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualsDifferentLevel() {
    CodecSpec a = CodecSpecParser.parse("ZSTD(3)");
    CodecSpec b = CodecSpecParser.parse("ZSTD(7)");
    Assert.assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualsWithAndWithoutLevel() {
    CodecSpec a = CodecSpecParser.parse("ZSTD");
    CodecSpec b = CodecSpecParser.parse("ZSTD(3)");
    Assert.assertNotEquals(a, b);
  }

  @Test
  public void testEqualsSelf() {
    CodecSpec spec = CodecSpecParser.parse("GZIP");
    Assert.assertEquals(spec, spec);
  }

  @Test
  public void testNotEqualsNull() {
    CodecSpec spec = CodecSpecParser.parse("GZIP");
    Assert.assertNotEquals(spec, null);
  }

  // ---- JSON serialization round-trip tests ----

  @Test
  public void testJsonRoundTripSimple()
      throws JsonProcessingException {
    CodecSpec original = CodecSpecParser.parse("SNAPPY");
    String json = JsonUtils.objectToString(original);
    Assert.assertEquals(json, "\"SNAPPY\"");

    CodecSpec deserialized = JsonUtils.stringToObject(json, CodecSpec.class);
    Assert.assertEquals(deserialized, original);
  }

  @Test
  public void testJsonRoundTripWithLevel()
      throws JsonProcessingException {
    CodecSpec original = CodecSpecParser.parse("ZSTD(3)");
    String json = JsonUtils.objectToString(original);
    Assert.assertEquals(json, "\"ZSTANDARD(3)\"");

    CodecSpec deserialized = JsonUtils.stringToObject(json, CodecSpec.class);
    Assert.assertEquals(deserialized, original);
  }

  @Test
  public void testJsonRoundTripAlias()
      throws JsonProcessingException {
    CodecSpec original = CodecSpecParser.parse("ZSTD");
    String json = JsonUtils.objectToString(original);
    Assert.assertEquals(json, "\"ZSTANDARD\"");

    CodecSpec deserialized = JsonUtils.stringToObject(json, CodecSpec.class);
    Assert.assertEquals(deserialized, original);
  }

  // ---- Factory method tests ----

  @Test
  public void testOfWithName() {
    CodecSpec spec = CodecSpec.of("GZIP");
    Assert.assertEquals(spec.getName(), "GZIP");
    Assert.assertTrue(spec.getParams().isEmpty());
    Assert.assertEquals(spec.getLevel(), OptionalInt.empty());
  }

  @Test
  public void testOfWithNameAndParams() {
    CodecSpec spec = CodecSpec.of("ZSTANDARD", Collections.singletonMap("level", "3"));
    Assert.assertEquals(spec.getName(), "ZSTANDARD");
    Assert.assertEquals(spec.getLevel(), OptionalInt.of(3));
  }

  @Test
  public void testParamsAreImmutable() {
    Map<String, String> mutable = new HashMap<>();
    mutable.put("level", "5");
    CodecSpec spec = CodecSpec.of("ZSTANDARD", mutable);

    // Mutating the original map should not affect the spec
    mutable.put("level", "99");
    Assert.assertEquals(spec.getLevel(), OptionalInt.of(5));
  }
}
