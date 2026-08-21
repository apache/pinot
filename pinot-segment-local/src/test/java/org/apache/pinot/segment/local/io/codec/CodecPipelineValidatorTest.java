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

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.pinot.segment.spi.codec.CodecPipeline;
import org.apache.pinot.segment.spi.codec.CodecSpecParser;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// Tests for [CodecPipelineValidator]. Synthetic transform handlers keep the structural rules
/// testable independently of the concrete transforms; the DELTA/DELTADELTA scenarios exercise the
/// real typed-layout-preserving transforms against [CodecRegistry#DEFAULT].
public class CodecPipelineValidatorTest {
  private static final TestCodecHandler TYPED_TRANSFORM =
      new TestCodecHandler("TYPED", CodecKind.TRANSFORM, true);
  private static final TestCodecHandler PACKING_TRANSFORM =
      new TestCodecHandler("PACKING", CodecKind.TRANSFORM, false);
  private static final CodecRegistry REGISTRY = new CodecRegistry()
      .register(TYPED_TRANSFORM)
      .register(PACKING_TRANSFORM)
      .register(Lz4CodecDefinition.INSTANCE)
      .register(SnappyCodecDefinition.INSTANCE)
      .register(GzipCodecDefinition.INSTANCE)
      .register(ZstdCodecDefinition.INSTANCE);

  @Test
  public void testCompressionPipelinesAreValid() {
    validate("LZ4", DataType.INT);
    validate("ZSTD(3)", DataType.LONG);
    validate("ZSTD", DataType.STRING);
    validate("LZ4,GZIP,SNAPPY,ZSTD(5)", DataType.BYTES);
  }

  @Test
  public void testTypedAndPackingTransformOrdering() {
    validate("TYPED", DataType.INT);
    validate("TYPED,TYPED,LZ4", DataType.INT);
    validate("TYPED,PACKING,ZSTD(3)", DataType.LONG);
    validate("PACKING,SNAPPY", DataType.STRING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTransformAfterCompressionRejected() {
    validate("ZSTD(3),TYPED", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTransformAfterPackingTransformRejected() {
    validate("PACKING,TYPED,LZ4", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTwoPackingTransformsRejected() {
    validate("PACKING,PACKING", DataType.LONG);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Unknown codec.*")
  public void testUnknownCodec() {
    validate("NOSUCHCODEC", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*out of range.*")
  public void testZstdLevelTooHigh() {
    validate("ZSTD(99)", DataType.INT);
  }

  @Test
  public void testZstdNegativeLevelIsOutsideTheUnsignedDslContract() {
    assertThrows(IllegalArgumentException.class,
        () -> ZstdCodecDefinition.INSTANCE.parseOptions(List.of("-1")));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testZstdBadArgCount() {
    validate("ZSTD(3,4)", DataType.INT);
  }

  @Test
  public void testTypedValueLayoutContract() {
    assertTrue(TYPED_TRANSFORM.preservesTypedValueLayout());
    assertFalse(PACKING_TRANSFORM.preservesTypedValueLayout());
    assertFalse(Lz4CodecDefinition.INSTANCE.preservesTypedValueLayout());
    // DELTA/DELTADELTA output a same-width typed value array, so they may be chained ahead of
    // another transform (or each other).
    assertTrue(DeltaCodecDefinition.INSTANCE.preservesTypedValueLayout());
    assertTrue(DeltaDeltaCodecDefinition.INSTANCE.preservesTypedValueLayout());
    // The packing transforms (T64/GORILLA) emit a headered byte frame, so they must return false.
    assertFalse(T64CodecDefinition.INSTANCE.preservesTypedValueLayout());
    assertFalse(GorillaCodecDefinition.INSTANCE.preservesTypedValueLayout());
  }

  // -------------------------------------------------------------------------
  // DELTA / DELTADELTA — the real typed-layout-preserving transforms, resolved
  // through the production CodecRegistry.DEFAULT
  // -------------------------------------------------------------------------

  @Test
  public void testDeltaPipelinesAreValid() {
    validateWithDefaultRegistry("DELTA", DataType.INT);
    validateWithDefaultRegistry("DELTA", DataType.LONG);
    validateWithDefaultRegistry("DELTADELTA", DataType.INT);
    validateWithDefaultRegistry("DELTADELTA", DataType.LONG);
    validateWithDefaultRegistry("DELTA,LZ4", DataType.INT);
    validateWithDefaultRegistry("DELTA,ZSTD(3)", DataType.INT);
    validateWithDefaultRegistry("DELTADELTA,SNAPPY", DataType.LONG);
    validateWithDefaultRegistry("DELTADELTA,ZSTD(8)", DataType.LONG);
  }

  /// Typed-layout-preserving transforms may be chained in any order, including with themselves,
  /// optionally followed by any number of compression stages.
  @Test
  public void testTypedLayoutTransformChainingAllowed() {
    validateWithDefaultRegistry("DELTA,DELTADELTA,LZ4", DataType.INT);
    validateWithDefaultRegistry("DELTA,DELTA,LZ4", DataType.INT);
    validateWithDefaultRegistry("DELTA,DELTADELTA", DataType.INT);
    validateWithDefaultRegistry("DELTADELTA,DELTA,ZSTD(3)", DataType.LONG);
    validateWithDefaultRegistry("DELTA,LZ4,ZSTD(3)", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testDeltaAfterCompressionRejected() {
    // ZSTD before DELTA is wrong: DELTA cannot consume compressed bytes
    validateWithDefaultRegistry("ZSTD(3),DELTA", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*DELTA codec only supports INT and LONG.*")
  public void testDeltaOnStringColumn() {
    validateWithDefaultRegistry("DELTA", DataType.STRING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*DELTA codec only supports INT and LONG.*")
  public void testDeltaOnDoubleColumn() {
    validateWithDefaultRegistry("DELTA", DataType.DOUBLE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*DELTADELTA codec only supports INT and LONG.*")
  public void testDeltaDeltaOnStringColumn() {
    validateWithDefaultRegistry("DELTADELTA", DataType.STRING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDeltaRejectsArguments() {
    validateWithDefaultRegistry("DELTA(1)", DataType.INT);
  }

  // -------------------------------------------------------------------------
  // T64 / GORILLA — the real packing transforms, resolved through the
  // production CodecRegistry.DEFAULT
  // -------------------------------------------------------------------------

  /// A typed-layout-preserving transform may precede a packing transform, which may precede
  /// compression(s).
  @Test
  public void testTypedLayoutThenPackingThenCompressionAllowed() {
    validateWithDefaultRegistry("DELTA,T64,LZ4", DataType.INT);
    validateWithDefaultRegistry("DELTADELTA,GORILLA,ZSTD(3)", DataType.LONG);
    validateWithDefaultRegistry("DELTA,T64", DataType.LONG);
  }

  /// A packing transform may stand alone or be followed by any number of compression stages.
  @Test
  public void testCompressionAfterPackingAllowed() {
    validateWithDefaultRegistry("T64", DataType.INT);
    validateWithDefaultRegistry("GORILLA", DataType.LONG);
    validateWithDefaultRegistry("T64,ZSTD(3)", DataType.LONG);
    validateWithDefaultRegistry("GORILLA,SNAPPY", DataType.LONG);
    validateWithDefaultRegistry("T64,LZ4,GZIP", DataType.INT);
  }

  /// A packing transform (T64/GORILLA) must be the last transform — another transform may not
  /// follow it because its output is a bit-packed (non-typed) byte stream.
  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testTransformAfterT64Rejected() {
    validateWithDefaultRegistry("T64,DELTA", DataType.INT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*must operate on column values.*")
  public void testT64ThenGorillaRejected() {
    validateWithDefaultRegistry("T64,GORILLA", DataType.LONG);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*T64 codec only supports INT and LONG.*")
  public void testT64OnStringColumn() {
    validateWithDefaultRegistry("T64", DataType.STRING);
  }

  @Test(expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*GORILLA codec only supports INT and LONG.*")
  public void testGorillaOnDoubleColumn() {
    validateWithDefaultRegistry("GORILLA", DataType.DOUBLE);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testT64RejectsArguments() {
    validateWithDefaultRegistry("T64(1)", DataType.INT);
  }

  private static void validate(String spec, DataType dataType) {
    CodecPipeline pipeline = CodecSpecParser.parse(spec);
    CodecPipelineValidator.validate(pipeline, REGISTRY, new CodecContext(dataType));
  }

  private static void validateWithDefaultRegistry(String spec, DataType dataType) {
    CodecPipeline pipeline = CodecSpecParser.parse(spec);
    CodecPipelineValidator.validate(pipeline, CodecRegistry.DEFAULT, new CodecContext(dataType));
  }

  private static final class TestCodecHandler implements ChunkCodecHandler<CodecOptions> {
    private static final CodecOptions OPTIONS = new CodecOptions() {
    };

    private final String _name;
    private final CodecKind _kind;
    private final boolean _preservesTypedValueLayout;

    private TestCodecHandler(String name, CodecKind kind, boolean preservesTypedValueLayout) {
      _name = name;
      _kind = kind;
      _preservesTypedValueLayout = preservesTypedValueLayout;
    }

    @Override
    public String name() {
      return _name;
    }

    @Override
    public CodecKind kind() {
      return _kind;
    }

    @Override
    public CodecOptions parseOptions(List<String> args) {
      if (!args.isEmpty()) {
        throw new IllegalArgumentException(_name + " does not accept arguments");
      }
      return OPTIONS;
    }

    @Override
    public void validateContext(CodecOptions options, CodecContext ctx) {
    }

    @Override
    public String canonicalize(CodecOptions options) {
      return _name;
    }

    @Override
    public boolean preservesTypedValueLayout() {
      return _preservesTypedValueLayout;
    }

    @Override
    public ByteBuffer encode(CodecOptions options, CodecContext ctx, ByteBuffer src) {
      throw new UnsupportedOperationException("Validation-only test codec");
    }

    @Override
    public ByteBuffer decode(CodecOptions options, CodecContext ctx, ByteBuffer src) {
      throw new UnsupportedOperationException("Validation-only test codec");
    }

    @Override
    public void decodeInto(CodecOptions options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) {
      throw new UnsupportedOperationException("Validation-only test codec");
    }

    @Override
    public int maxEncodedSize(CodecOptions options, int inputSize) {
      return inputSize;
    }

    @Override
    public boolean requiresDirectDstBuffer() {
      return false;
    }
  }
}
