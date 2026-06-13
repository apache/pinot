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
package org.apache.pinot.segment.local.io.codec;

import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecPipeline;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;


/// Tests for [CodecPipelineValidator].
public class CodecPipelineValidatorTest {

  private static final CodecRegistry REGISTRY = CodecRegistry.DEFAULT;

  // -------------------------------------------------------------------------
  // Valid pipelines
  // -------------------------------------------------------------------------

  @Test
  public void testValidDeltaOnly() {
    validate("DELTA", DataType.INT);
    validate("DELTA", DataType.LONG);
  }

  @Test
  public void testValidDeltaDeltaOnly() {
    validate("DELTADELTA", DataType.INT);
    validate("DELTADELTA", DataType.LONG);
  }

  @Test
  public void testValidDeltaDeltaThenLz4() {
    validate("CODEC(DELTADELTA,LZ4)", DataType.INT);
    validate("CODEC(DELTADELTA,LZ4)", DataType.LONG);
  }

  @Test
  public void testValidZstdOnly() {
    validate("ZSTD(3)", DataType.INT);
    validate("ZSTD(3)", DataType.LONG);
    validate("ZSTD", DataType.STRING);
  }

  @Test
  public void testValidDeltaThenZstd() {
    validate("CODEC(DELTA,ZSTD(3))", DataType.INT);
    validate("CODEC(DELTA,ZSTD(8))", DataType.LONG);
  }

  // -------------------------------------------------------------------------
  // Invalid: ordering violation — a transform cannot follow a compression stage
  // -------------------------------------------------------------------------

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTransformAfterCompressionRejected() {
    // ZSTD before DELTA is wrong: DELTA cannot consume compressed bytes
    validate("CODEC(ZSTD(3),DELTA)", DataType.INT);
  }

  // -------------------------------------------------------------------------
  // Invalid: type check
  // -------------------------------------------------------------------------

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*DELTA codec only supports INT and LONG.*")
  public void testDeltaOnStringColumn() {
    validate("DELTA", DataType.STRING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*DELTA codec only supports INT and LONG.*")
  public void testDeltaOnDoubleColumn() {
    validate("DELTA", DataType.DOUBLE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*DELTADELTA codec only supports INT and LONG.*")
  public void testDeltaDeltaOnStringColumn() {
    validate("DELTADELTA", DataType.STRING);
  }

  // -------------------------------------------------------------------------
  // Valid: chaining — N value transforms → optional packing transform → N compressions
  // -------------------------------------------------------------------------

  /// Multiple value-preserving transforms may be chained, optionally followed by compression.
  @Test
  public void testMultipleValueTransformsAllowed() {
    validate("CODEC(DELTA,DELTADELTA,LZ4)", DataType.INT);
    validate("CODEC(DELTA,DELTA,LZ4)", DataType.INT);
    validate("CODEC(DELTA,DELTADELTA)", DataType.INT);
    validate("CODEC(DELTADELTA,DELTA,ZSTD(3))", DataType.LONG);
  }

  /// A value transform may precede a packing transform, which may precede compression(s).
  @Test
  public void testValueThenPackingThenCompressionAllowed() {
    validate("CODEC(DELTA,T64,LZ4)", DataType.INT);
    validate("CODEC(DELTADELTA,GORILLA,ZSTD(3))", DataType.LONG);
    validate("CODEC(DELTA,T64)", DataType.LONG);
  }

  /// Multiple compression stages may be chained after the transforms.
  @Test
  public void testMultipleCompressionsAllowed() {
    validate("CODEC(DELTA,LZ4,ZSTD(3))", DataType.INT);
    validate("CODEC(LZ4,GZIP)", DataType.STRING);
  }

  /// Single transform + compression remains valid.
  @Test
  public void testSingleTransformPlusCompressionAllowed() {
    validate("CODEC(DELTA,LZ4)", DataType.INT);
    validate("CODEC(T64,ZSTD(3))", DataType.LONG);
    validate("CODEC(GORILLA,SNAPPY)", DataType.LONG);
  }

  /// A packing transform (T64/GORILLA) must be the last transform — another transform may not
  /// follow it because its output is a bit-packed (non-typed) byte stream.
  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTransformAfterPackingTransformRejected() {
    validate("CODEC(T64,DELTA,LZ4)", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTwoPackingTransformsRejected() {
    validate("CODEC(T64,GORILLA)", DataType.LONG);
  }

  // -------------------------------------------------------------------------
  // Invalid: unknown codec
  // -------------------------------------------------------------------------

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Unknown codec.*")
  public void testUnknownCodec() {
    validate("NOSUCHCODEC", DataType.INT);
  }

  // -------------------------------------------------------------------------
  // Invalid: ZSTD level out of range
  // -------------------------------------------------------------------------

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*out of range.*")
  public void testZstdLevelTooHigh() {
    validate("ZSTD(99)", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testZstdBadArgCount() {
    validate("ZSTD(3,4)", DataType.INT);
  }

  // -------------------------------------------------------------------------

  private static void validate(String spec, DataType dataType) {
    CodecPipeline pipeline = CodecSpecParser.parse(spec);
    CodecPipelineValidator.validate(pipeline, REGISTRY, new CodecContext(dataType));
  }
}
