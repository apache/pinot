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
package org.apache.pinot.segment.spi.codec;

import java.util.Arrays;
import java.util.Collections;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for [CodecSpecParser].
public class CodecSpecParserTest {

  // -------------------------------------------------------------------------
  // Single-stage invocations
  // -------------------------------------------------------------------------

  @Test
  public void testSingleCodecNoArgs() {
    CodecPipeline pipeline = CodecSpecParser.parse("DELTA");
    assertEquals(pipeline.stages().size(), 1);
    CodecInvocation inv = pipeline.stages().get(0);
    assertEquals(inv.name(), "DELTA");
    assertTrue(inv.args().isEmpty());
    assertEquals(pipeline.toDslString(), "DELTA");
  }

  @Test
  public void testSingleCodecWithOneArg() {
    CodecPipeline pipeline = CodecSpecParser.parse("ZSTD(3)");
    assertEquals(pipeline.stages().size(), 1);
    CodecInvocation inv = pipeline.stages().get(0);
    assertEquals(inv.name(), "ZSTD");
    assertEquals(inv.args(), Collections.singletonList("3"));
    assertEquals(pipeline.toDslString(), "ZSTD(3)");
  }

  @Test
  public void testSingleCodecWithMultipleArgs() {
    CodecPipeline pipeline = CodecSpecParser.parse("FPC(12,4)");
    assertEquals(pipeline.stages().size(), 1);
    CodecInvocation inv = pipeline.stages().get(0);
    assertEquals(inv.name(), "FPC");
    assertEquals(inv.args(), Arrays.asList("12", "4"));
    assertEquals(pipeline.toDslString(), "FPC(12,4)");
  }

  @Test
  public void testSingleCodecUpperCaseNormalized() {
    // Parser should upper-case the name
    CodecPipeline pipeline = CodecSpecParser.parse("zstd(3)");
    assertEquals(pipeline.stages().get(0).name(), "ZSTD");
  }

  // -------------------------------------------------------------------------
  // Multi-stage pipelines
  // -------------------------------------------------------------------------

  @Test
  public void testMultiStagePipeline() {
    CodecPipeline pipeline = CodecSpecParser.parse("CODEC(DELTA,ZSTD(8))");
    assertEquals(pipeline.stages().size(), 2);
    assertEquals(pipeline.stages().get(0).name(), "DELTA");
    assertTrue(pipeline.stages().get(0).args().isEmpty());
    assertEquals(pipeline.stages().get(1).name(), "ZSTD");
    assertEquals(pipeline.stages().get(1).args(), Collections.singletonList("8"));
    assertEquals(pipeline.toDslString(), "CODEC(DELTA,ZSTD(8))");
  }

  @Test
  public void testMultiStagePipelineWithSpaces() {
    CodecPipeline pipeline = CodecSpecParser.parse("CODEC( DELTA , ZSTD( 3 ) )");
    assertEquals(pipeline.stages().size(), 2);
    assertEquals(pipeline.stages().get(0).name(), "DELTA");
    assertEquals(pipeline.stages().get(1).name(), "ZSTD");
    assertEquals(pipeline.stages().get(1).args(), Collections.singletonList("3"));
  }

  @Test
  public void testSingleStagePipelineCanonicalHasNoOuterWrapper() {
    // A CODEC() wrapper around a single stage should parse fine but
    // the canonical form of a single-stage pipeline has no wrapper.
    CodecPipeline pipeline = CodecSpecParser.parse("CODEC(ZSTD(3))");
    assertEquals(pipeline.stages().size(), 1);
    // toDslString() on single stage: "ZSTD(3)", not "CODEC(ZSTD(3))"
    assertEquals(pipeline.toDslString(), "ZSTD(3)");
  }

  // -------------------------------------------------------------------------
  // Invalid cases
  // -------------------------------------------------------------------------

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNullSpec() {
    CodecSpecParser.parse(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBlankSpec() {
    CodecSpecParser.parse("  ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testUnmatchedOpenParen() {
    CodecSpecParser.parse("ZSTD(3");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testTrailingGarbage() {
    CodecSpecParser.parse("DELTA extra");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testNonIntegerArg() {
    CodecSpecParser.parse("ZSTD(abc)");
  }

  /// Regression: `Character.isDigit` accepts non-ASCII digits (e.g. Devanagari U+0966) which
  /// `Integer.parseInt` later rejects with a confusing message. The parser must reject these at
  /// the grammar level so the on-disk canonical spec stays locale-stable.
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testRejectsNonAsciiDigit() {
    CodecSpecParser.parse("ZSTD(०)"); // Devanagari ZERO
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyPipeline() {
    // CODEC with no stages is invalid
    CodecSpecParser.parse("CODEC()");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMissingCloseParen() {
    CodecSpecParser.parse("CODEC(DELTA,ZSTD(3)");
  }

  // -------------------------------------------------------------------------
  // CodecPipeline equality
  // -------------------------------------------------------------------------

  @Test
  public void testPipelineEquality() {
    CodecPipeline a = CodecSpecParser.parse("CODEC(DELTA,ZSTD(3))");
    CodecPipeline b = CodecSpecParser.parse("CODEC(DELTA,ZSTD(3))");
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testPipelineInequalityDifferentArgs() {
    CodecPipeline a = CodecSpecParser.parse("ZSTD(3)");
    CodecPipeline b = CodecSpecParser.parse("ZSTD(8)");
    assertNotEquals(a, b);
  }
}
