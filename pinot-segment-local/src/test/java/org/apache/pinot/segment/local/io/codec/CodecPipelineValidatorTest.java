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


/// Tests for [CodecPipelineValidator]. Synthetic transform handlers keep this layer independent
/// from the concrete transform implementations introduced by later stacked pull requests.
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
  }

  private static void validate(String spec, DataType dataType) {
    CodecPipeline pipeline = CodecSpecParser.parse(spec);
    CodecPipelineValidator.validate(pipeline, REGISTRY, new CodecContext(dataType));
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
