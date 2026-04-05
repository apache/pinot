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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.io.codec.compression.ChunkCompressorFactory;
import org.apache.pinot.segment.local.io.codec.transform.DeltaTransform;
import org.apache.pinot.segment.local.io.codec.transform.DoubleDeltaTransform;
import org.apache.pinot.segment.local.io.codec.transform.XorTransform;
import org.apache.pinot.segment.local.io.writer.impl.FixedByteChunkForwardIndexWriter;
import org.apache.pinot.segment.local.segment.index.readers.forward.ChunkReaderContext;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.spi.codec.ChunkCodec;
import org.apache.pinot.segment.spi.codec.ChunkCodecPipeline;
import org.apache.pinot.segment.spi.codec.ChunkTransform;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.compression.ChunkCompressor;
import org.apache.pinot.segment.spi.compression.ChunkDecompressor;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/**
 * Tests for codec pipeline: {@link ChunkCodecPipeline}, {@link PipelineChunkCompressor},
 * {@link PipelineChunkDecompressor}, {@link DeltaTransform}, and {@link DoubleDeltaTransform}.
 */
public class ChunkCodecPipelineTest {

  // ===== ChunkCodecPipeline construction tests =====

  @Test
  public void testSingleCompressor() {
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(Collections.singletonList(ChunkCodec.ZSTANDARD));
    assertEquals(pipeline.size(), 1);
    assertEquals(pipeline.getCompressor(), ChunkCodec.ZSTANDARD);
    assertFalse(pipeline.hasTransforms());
    assertTrue(pipeline.getTransforms().isEmpty());
  }

  @Test
  public void testTransformPlusCompressor() {
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(
        Arrays.asList(ChunkCodec.DELTA, ChunkCodec.ZSTANDARD));
    assertEquals(pipeline.size(), 2);
    assertEquals(pipeline.getCompressor(), ChunkCodec.ZSTANDARD);
    assertTrue(pipeline.hasTransforms());
    assertEquals(pipeline.getTransforms(), Collections.singletonList(ChunkCodec.DELTA));
  }

  @Test
  public void testMultipleTransforms() {
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(
        Arrays.asList(ChunkCodec.DELTA, ChunkCodec.DOUBLE_DELTA, ChunkCodec.LZ4));
    assertEquals(pipeline.size(), 3);
    assertEquals(pipeline.getCompressor(), ChunkCodec.LZ4);
    assertEquals(pipeline.getTransforms(), Arrays.asList(ChunkCodec.DELTA, ChunkCodec.DOUBLE_DELTA));
  }

  @Test
  public void testTransformOnly() {
    // Pipeline with only transforms — compressor defaults to PASS_THROUGH
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(
        Collections.singletonList(ChunkCodec.DELTA));
    assertEquals(pipeline.getCompressor(), ChunkCodec.PASS_THROUGH);
    assertTrue(pipeline.hasTransforms());
  }

  @Test
  public void testCompressorNotLastFails() {
    expectThrows(IllegalArgumentException.class, () ->
        new ChunkCodecPipeline(Arrays.asList(ChunkCodec.ZSTANDARD, ChunkCodec.DELTA)));
  }

  @Test
  public void testMultipleCompressorsFails() {
    expectThrows(IllegalArgumentException.class, () ->
        new ChunkCodecPipeline(Arrays.asList(ChunkCodec.SNAPPY, ChunkCodec.ZSTANDARD)));
  }

  @Test
  public void testEmptyPipelineFails() {
    expectThrows(IllegalArgumentException.class, () ->
        new ChunkCodecPipeline(Collections.emptyList()));
  }

  @Test
  public void testFromNames() {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD"));
    assertEquals(pipeline.getStages(), Arrays.asList(ChunkCodec.DELTA, ChunkCodec.ZSTANDARD));
  }

  @Test
  public void testFromNamesRejectsLegacyCompound() {
    // DELTA_LZ4 and DOUBLE_DELTA_LZ4 are internal-only — users must use ["DELTA", "LZ4"] instead
    expectThrows(IllegalArgumentException.class, () ->
        ChunkCodecPipeline.fromNames(Collections.singletonList("DELTA_LZ4")));
    expectThrows(IllegalArgumentException.class, () ->
        ChunkCodecPipeline.fromNames(Collections.singletonList("DOUBLE_DELTA_LZ4")));
  }

  @Test
  public void testFromValues() {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromValues(new int[]{100, 2});
    assertEquals(pipeline.getStages(), Arrays.asList(ChunkCodec.DELTA, ChunkCodec.ZSTANDARD));
  }

  @Test
  public void testEquality() {
    ChunkCodecPipeline a = ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD"));
    ChunkCodecPipeline b = ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD"));
    ChunkCodecPipeline c = ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "LZ4"));
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertFalse(a.equals(c));
  }

  // ===== DeltaTransform tests =====

  @Test
  public void testDeltaEncodeDecodeInts() {
    int[] values = {100, 105, 108, 120, 125, 130};
    assertRoundTripInts(DeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDeltaEncodeDecodeLongs() {
    long[] values = {1000000L, 1000050L, 1000100L, 1000200L, 1000250L};
    assertRoundTripLongs(DeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDeltaEmptyInts() {
    assertRoundTripInts(DeltaTransform.INSTANCE, new int[0]);
  }

  @Test
  public void testDeltaSingleInt() {
    assertRoundTripInts(DeltaTransform.INSTANCE, new int[]{42});
  }

  @Test
  public void testDeltaNegativeValues() {
    int[] values = {-100, -50, 0, 50, 100, -200};
    assertRoundTripInts(DeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDeltaOverflowInt() {
    int[] values = {Integer.MAX_VALUE, Integer.MIN_VALUE, 0, Integer.MAX_VALUE};
    assertRoundTripInts(DeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDeltaOverflowLong() {
    long[] values = {Long.MAX_VALUE, Long.MIN_VALUE, 0L, Long.MAX_VALUE};
    assertRoundTripLongs(DeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDeltaRandomInts() {
    Random random = new Random(12345);
    int[] values = new int[500];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextInt();
    }
    assertRoundTripInts(DeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDeltaRandomLongs() {
    Random random = new Random(12345);
    long[] values = new long[500];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextLong();
    }
    assertRoundTripLongs(DeltaTransform.INSTANCE, values);
  }

  // ===== DoubleDeltaTransform tests =====

  @Test
  public void testDoubleDeltaEncodeDecodeInts() {
    int[] values = {100, 110, 120, 130, 140, 150};
    assertRoundTripInts(DoubleDeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDoubleDeltaEncodeDecodeLongs() {
    long[] values = {1000L, 1100L, 1200L, 1300L, 1400L};
    assertRoundTripLongs(DoubleDeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDoubleDeltaTwoInts() {
    assertRoundTripInts(DoubleDeltaTransform.INSTANCE, new int[]{10, 20});
  }

  @Test
  public void testDoubleDeltaOverflowInt() {
    int[] values = {Integer.MAX_VALUE, Integer.MIN_VALUE, 0, Integer.MAX_VALUE};
    assertRoundTripInts(DoubleDeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDoubleDeltaConstantStep() {
    int[] values = new int[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = 1000 + i * 60;
    }
    assertRoundTripInts(DoubleDeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDoubleDeltaRandomInts() {
    Random random = new Random(67890);
    int[] values = new int[500];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextInt();
    }
    assertRoundTripInts(DoubleDeltaTransform.INSTANCE, values);
  }

  @Test
  public void testDoubleDeltaRandomLongs() {
    Random random = new Random(67890);
    long[] values = new long[500];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextLong();
    }
    assertRoundTripLongs(DoubleDeltaTransform.INSTANCE, values);
  }

  // ===== Pipeline compressor/decompressor round-trip tests =====

  @Test
  public void testPipelineDeltaLz4RoundTripInts()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "LZ4"));
    int[] original = {100, 200, 300, 400, 500, 600, 700, 800};
    assertPipelineRoundTripInts(pipeline, original);
  }

  @Test
  public void testPipelineDeltaZstdRoundTripLongs()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD"));
    long[] original = {1000L, 2000L, 3000L, 4000L, 5000L};
    assertPipelineRoundTripLongs(pipeline, original);
  }

  @Test
  public void testPipelineDoubleDeltaSnappyRoundTripInts()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(Arrays.asList("DOUBLE_DELTA", "SNAPPY"));
    int[] original = new int[100];
    for (int i = 0; i < original.length; i++) {
      original[i] = 1000 + i * 10;
    }
    assertPipelineRoundTripInts(pipeline, original);
  }

  @Test
  public void testPipelineMultiTransformRoundTrip()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(
        Arrays.asList("DELTA", "DOUBLE_DELTA", "ZSTANDARD"));
    int[] original = new int[200];
    for (int i = 0; i < original.length; i++) {
      original[i] = 1000 + i * 5;
    }
    assertPipelineRoundTripInts(pipeline, original);
  }

  @Test
  public void testPipelinePassThroughRoundTripInts()
      throws IOException {
    ChunkCodecPipeline pipeline = new ChunkCodecPipeline(
        Collections.singletonList(ChunkCodec.PASS_THROUGH));
    int[] original = {42, 43, 44, 45};
    assertPipelineRoundTripInts(pipeline, original);
  }

  // ===== XOR (Gorilla-style) transform tests =====

  @Test
  public void testXorEncodeDecodeFloats() {
    float[] floats = {72.3f, 72.4f, 72.5f, 72.6f, 72.7f};
    int numValues = floats.length;
    ByteBuffer buffer = ByteBuffer.allocate(numValues * Float.BYTES);
    for (float f : floats) {
      buffer.putFloat(f);
    }
    buffer.flip();

    XorTransform.INSTANCE.encode(buffer, buffer.remaining(), Float.BYTES);
    // First value unchanged, subsequent values should be XOR (small diffs → many zero bits)
    buffer.position(0);
    assertEquals(Float.intBitsToFloat(buffer.getInt(0)), floats[0]);

    XorTransform.INSTANCE.decode(buffer, numValues * Float.BYTES, Float.BYTES);
    buffer.position(0);
    for (float expected : floats) {
      assertEquals(buffer.getFloat(), expected);
    }
  }

  @Test
  public void testXorEncodeDecodeDoubles() {
    double[] doubles = {98.6, 98.7, 98.8, 98.9, 99.0};
    int numValues = doubles.length;
    ByteBuffer buffer = ByteBuffer.allocate(numValues * Double.BYTES);
    for (double d : doubles) {
      buffer.putDouble(d);
    }
    buffer.flip();

    XorTransform.INSTANCE.encode(buffer, buffer.remaining(), Double.BYTES);
    buffer.position(0);
    assertEquals(Double.longBitsToDouble(buffer.getLong(0)), doubles[0]);

    XorTransform.INSTANCE.decode(buffer, numValues * Double.BYTES, Double.BYTES);
    buffer.position(0);
    for (double expected : doubles) {
      assertEquals(buffer.getDouble(), expected);
    }
  }

  @Test
  public void testXorSingleFloat() {
    float[] floats = {42.5f};
    ByteBuffer buffer = ByteBuffer.allocate(Float.BYTES);
    buffer.putFloat(floats[0]);
    buffer.flip();
    XorTransform.INSTANCE.encode(buffer, Float.BYTES, Float.BYTES);
    XorTransform.INSTANCE.decode(buffer, Float.BYTES, Float.BYTES);
    buffer.position(0);
    assertEquals(buffer.getFloat(), floats[0]);
  }

  @Test
  public void testXorEmptyValues() {
    // Empty buffer should not throw
    ByteBuffer buffer = ByteBuffer.allocate(0);
    XorTransform.INSTANCE.encode(buffer, 0, Float.BYTES);
    XorTransform.INSTANCE.decode(buffer, 0, Float.BYTES);
  }

  @Test
  public void testXorRandomDoubles() {
    Random rng = new Random(42);
    int numValues = 500;
    double[] values = new double[numValues];
    values[0] = rng.nextDouble() * 100;
    for (int i = 1; i < numValues; i++) {
      // Small perturbations — XOR produces values with many zero bits
      values[i] = values[i - 1] + (rng.nextDouble() - 0.5);
    }
    ByteBuffer buffer = ByteBuffer.allocate(numValues * Double.BYTES);
    for (double v : values) {
      buffer.putDouble(v);
    }
    buffer.flip();
    XorTransform.INSTANCE.encode(buffer, numValues * Double.BYTES, Double.BYTES);
    XorTransform.INSTANCE.decode(buffer, numValues * Double.BYTES, Double.BYTES);
    buffer.position(0);
    for (double expected : values) {
      assertEquals(buffer.getDouble(), expected);
    }
  }

  @Test
  public void testPipelineXorLz4RoundTripFloats()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(Arrays.asList("XOR", "LZ4"));
    float[] floats = {72.3f, 72.4f, 72.5f, 72.6f, 72.7f, 72.8f};
    assertPipelineRoundTripFloats(pipeline, floats);
  }

  @Test
  public void testPipelineXorZstdRoundTripDoubles()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(Arrays.asList("XOR", "ZSTANDARD"));
    double[] doubles = {98.6, 98.7, 98.8, 98.9, 99.0};
    assertPipelineRoundTripDoubles(pipeline, doubles);
  }

  // ===== supportedTypes / validateStoredType tests =====

  @Test
  public void testDeltaSupportedTypes() {
    assertTrue(DeltaTransform.INSTANCE.supportedTypes().contains(DataType.INT));
    assertTrue(DeltaTransform.INSTANCE.supportedTypes().contains(DataType.LONG));
    assertFalse(DeltaTransform.INSTANCE.supportedTypes().contains(DataType.FLOAT));
    assertFalse(DeltaTransform.INSTANCE.supportedTypes().contains(DataType.DOUBLE));
    // Should not throw for supported types
    DeltaTransform.INSTANCE.validateStoredType(DataType.INT, "col");
    DeltaTransform.INSTANCE.validateStoredType(DataType.LONG, "col");
    // Should throw for unsupported types
    expectThrows(IllegalArgumentException.class,
        () -> DeltaTransform.INSTANCE.validateStoredType(DataType.FLOAT, "col"));
    expectThrows(IllegalArgumentException.class,
        () -> DeltaTransform.INSTANCE.validateStoredType(DataType.DOUBLE, "col"));
    expectThrows(IllegalArgumentException.class,
        () -> DeltaTransform.INSTANCE.validateStoredType(DataType.STRING, "col"));
  }

  @Test
  public void testDoubleDeltaSupportedTypes() {
    assertTrue(DoubleDeltaTransform.INSTANCE.supportedTypes().contains(DataType.INT));
    assertTrue(DoubleDeltaTransform.INSTANCE.supportedTypes().contains(DataType.LONG));
    assertFalse(DoubleDeltaTransform.INSTANCE.supportedTypes().contains(DataType.FLOAT));
    assertFalse(DoubleDeltaTransform.INSTANCE.supportedTypes().contains(DataType.DOUBLE));
    DoubleDeltaTransform.INSTANCE.validateStoredType(DataType.INT, "col");
    DoubleDeltaTransform.INSTANCE.validateStoredType(DataType.LONG, "col");
    expectThrows(IllegalArgumentException.class,
        () -> DoubleDeltaTransform.INSTANCE.validateStoredType(DataType.FLOAT, "col"));
  }

  @Test
  public void testXorSupportedTypes() {
    assertTrue(XorTransform.INSTANCE.supportedTypes().contains(DataType.FLOAT));
    assertTrue(XorTransform.INSTANCE.supportedTypes().contains(DataType.DOUBLE));
    assertFalse(XorTransform.INSTANCE.supportedTypes().contains(DataType.INT));
    assertFalse(XorTransform.INSTANCE.supportedTypes().contains(DataType.LONG));
    XorTransform.INSTANCE.validateStoredType(DataType.FLOAT, "col");
    XorTransform.INSTANCE.validateStoredType(DataType.DOUBLE, "col");
    expectThrows(IllegalArgumentException.class,
        () -> XorTransform.INSTANCE.validateStoredType(DataType.INT, "col"));
    expectThrows(IllegalArgumentException.class,
        () -> XorTransform.INSTANCE.validateStoredType(DataType.LONG, "col"));
    expectThrows(IllegalArgumentException.class,
        () -> XorTransform.INSTANCE.validateStoredType(DataType.STRING, "col"));
  }

  // ===== ChunkCodec enum tests =====

  @Test
  public void testChunkCodecFromValue() {
    assertEquals(ChunkCodec.fromValue(0), ChunkCodec.PASS_THROUGH);
    assertEquals(ChunkCodec.fromValue(2), ChunkCodec.ZSTANDARD);
    assertEquals(ChunkCodec.fromValue(6), ChunkCodec.DELTA_LZ4);
    assertEquals(ChunkCodec.fromValue(7), ChunkCodec.DOUBLE_DELTA_LZ4);
    assertEquals(ChunkCodec.fromValue(100), ChunkCodec.DELTA);
    assertEquals(ChunkCodec.fromValue(101), ChunkCodec.DOUBLE_DELTA);
  }

  @Test
  public void testChunkCodecKind() {
    assertTrue(ChunkCodec.ZSTANDARD.isCompressor());
    assertFalse(ChunkCodec.ZSTANDARD.isTransform());
    assertTrue(ChunkCodec.DELTA.isTransform());
    assertFalse(ChunkCodec.DELTA.isCompressor());
    // Legacy compound codecs are compressors and internal-only
    assertTrue(ChunkCodec.DELTA_LZ4.isCompressor());
    assertFalse(ChunkCodec.DELTA_LZ4.isTransform());
    assertTrue(ChunkCodec.DELTA_LZ4.isInternalOnly());
    assertTrue(ChunkCodec.DOUBLE_DELTA_LZ4.isCompressor());
    assertFalse(ChunkCodec.DOUBLE_DELTA_LZ4.isTransform());
    assertTrue(ChunkCodec.DOUBLE_DELTA_LZ4.isInternalOnly());
    // User-facing codecs are not internal-only
    assertFalse(ChunkCodec.LZ4.isInternalOnly());
    assertFalse(ChunkCodec.DELTA.isInternalOnly());
    assertFalse(ChunkCodec.DOUBLE_DELTA.isInternalOnly());
  }

  @Test
  public void testChunkCodecInvalidValue() {
    expectThrows(IllegalArgumentException.class, () -> ChunkCodec.fromValue(999));
  }

  // ===== fromCompressionType tests for legacy compound codecs =====

  @Test
  public void testFromCompressionTypeDelta() {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromCompressionType(ChunkCompressionType.DELTA);
    assertEquals(pipeline.size(), 1);
    assertEquals(pipeline.get(0), ChunkCodec.DELTA_LZ4);
    assertTrue(pipeline.get(0).isCompressor());
    assertFalse(pipeline.hasTransforms());
    assertEquals(pipeline.getChunkCompressionType(), ChunkCompressionType.DELTA);
  }

  @Test
  public void testFromCompressionTypeDeltaDelta() {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromCompressionType(ChunkCompressionType.DELTADELTA);
    assertEquals(pipeline.size(), 1);
    assertEquals(pipeline.get(0), ChunkCodec.DOUBLE_DELTA_LZ4);
    assertTrue(pipeline.get(0).isCompressor());
    assertFalse(pipeline.hasTransforms());
    assertEquals(pipeline.getChunkCompressionType(), ChunkCompressionType.DELTADELTA);
  }

  @Test
  public void testFromCompressionTypeLz4() {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromCompressionType(ChunkCompressionType.LZ4);
    assertEquals(pipeline.size(), 1);
    assertEquals(pipeline.get(0), ChunkCodec.LZ4);
    assertFalse(pipeline.hasTransforms());
  }

  @Test
  public void testLegacyDeltaNotEqualToPipelineDelta() {
    // Legacy compound [DELTA_LZ4] is NOT the same as pipeline [DELTA, LZ4]
    ChunkCodecPipeline legacyDelta = ChunkCodecPipeline.fromCompressionType(ChunkCompressionType.DELTA);
    ChunkCodecPipeline pipelineDelta = new ChunkCodecPipeline(Arrays.asList(ChunkCodec.DELTA, ChunkCodec.LZ4));
    assertFalse(legacyDelta.equals(pipelineDelta));
  }

  // ===== Triple-delta (DELTA → DELTA → DELTA) stacking tests =====

  @Test
  public void testTripleDeltaInts() {
    // Stacking 3 delta transforms: should round-trip correctly
    int[] values = new int[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = 1000 + i * 7;
    }
    assertChainedTransformRoundTripInts(
        new ChunkTransform[]{DeltaTransform.INSTANCE, DeltaTransform.INSTANCE, DeltaTransform.INSTANCE}, values);
  }

  @Test
  public void testTripleDeltaLongs() {
    long[] values = new long[100];
    for (int i = 0; i < values.length; i++) {
      values[i] = 50000L + i * 13L;
    }
    assertChainedTransformRoundTripLongs(
        new ChunkTransform[]{DeltaTransform.INSTANCE, DeltaTransform.INSTANCE, DeltaTransform.INSTANCE}, values);
  }

  @Test
  public void testTripleDeltaRandomInts() {
    Random random = new Random(77777);
    int[] values = new int[500];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextInt();
    }
    assertChainedTransformRoundTripInts(
        new ChunkTransform[]{DeltaTransform.INSTANCE, DeltaTransform.INSTANCE, DeltaTransform.INSTANCE}, values);
  }

  @Test
  public void testDeltaDoubleDeltaDeltaMixInts() {
    // Mixed: DELTA → DOUBLE_DELTA → DELTA
    int[] values = new int[200];
    for (int i = 0; i < values.length; i++) {
      values[i] = 1000 + i * 3 + (i % 5);
    }
    assertChainedTransformRoundTripInts(
        new ChunkTransform[]{DeltaTransform.INSTANCE, DoubleDeltaTransform.INSTANCE, DeltaTransform.INSTANCE}, values);
  }

  @Test
  public void testDoubleDeltaDoubleDeltaInts() {
    // DOUBLE_DELTA → DOUBLE_DELTA (stacked)
    int[] values = new int[200];
    for (int i = 0; i < values.length; i++) {
      values[i] = 5000 + i * i;
    }
    assertChainedTransformRoundTripInts(
        new ChunkTransform[]{DoubleDeltaTransform.INSTANCE, DoubleDeltaTransform.INSTANCE}, values);
  }

  // ===== Pipeline compressor/decompressor: triple-delta + compression round-trip =====

  @Test
  public void testPipelineTripleDeltaZstdRoundTripInts()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(
        Arrays.asList("DELTA", "DELTA", "DELTA", "ZSTANDARD"));
    int[] original = new int[200];
    for (int i = 0; i < original.length; i++) {
      original[i] = 1000 + i * 7;
    }
    assertPipelineRoundTripInts(pipeline, original);
  }

  @Test
  public void testPipelineTripleDeltaLz4RoundTripLongs()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(
        Arrays.asList("DELTA", "DELTA", "DELTA", "LZ4"));
    long[] original = new long[200];
    for (int i = 0; i < original.length; i++) {
      original[i] = 50000L + i * 13L;
    }
    assertPipelineRoundTripLongs(pipeline, original);
  }

  @Test
  public void testPipelineTripleDeltaRandomInts()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(
        Arrays.asList("DELTA", "DELTA", "DELTA", "ZSTANDARD"));
    Random random = new Random(88888);
    int[] original = new int[1000];
    for (int i = 0; i < original.length; i++) {
      original[i] = random.nextInt();
    }
    assertPipelineRoundTripInts(pipeline, original);
  }

  @Test
  public void testPipelineDeltaDoubleDeltaDeltaSnappyRoundTripInts()
      throws IOException {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(
        Arrays.asList("DELTA", "DOUBLE_DELTA", "DELTA", "SNAPPY"));
    int[] original = new int[300];
    for (int i = 0; i < original.length; i++) {
      original[i] = 1000 + i * 3 + (i % 5);
    }
    assertPipelineRoundTripInts(pipeline, original);
  }

  // ===== End-to-end: write V7 file with pipeline, read back via reader =====

  private static final int E2E_NUM_VALUES = 10009;
  private static final int E2E_NUM_DOCS_PER_CHUNK = 5003;
  private static final String E2E_TEST_FILE =
      System.getProperty("java.io.tmpdir") + File.separator + "CodecPipelineE2E";

  @Test
  public void testE2eDeltaZstdInts()
      throws Exception {
    assertE2eWriteReadInts(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD")),
        generateMonotonicInts(E2E_NUM_VALUES, 1000, 7));
  }

  @Test
  public void testE2eDoubleDeltaLz4Ints()
      throws Exception {
    assertE2eWriteReadInts(
        ChunkCodecPipeline.fromNames(Arrays.asList("DOUBLE_DELTA", "LZ4")),
        generateMonotonicInts(E2E_NUM_VALUES, 5000, 10));
  }

  @Test
  public void testE2eTripleDeltaZstdInts()
      throws Exception {
    assertE2eWriteReadInts(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "DELTA", "DELTA", "ZSTANDARD")),
        generateMonotonicInts(E2E_NUM_VALUES, 0, 3));
  }

  @Test
  public void testE2eDeltaDoubleDeltaSnappyInts()
      throws Exception {
    assertE2eWriteReadInts(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "DOUBLE_DELTA", "SNAPPY")),
        generateMonotonicInts(E2E_NUM_VALUES, 100, 5));
  }

  @Test
  public void testE2eDeltaZstdLongs()
      throws Exception {
    assertE2eWriteReadLongs(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD")),
        generateMonotonicLongs(E2E_NUM_VALUES, 1000000L, 60000L));
  }

  @Test
  public void testE2eTripleDeltaLz4Longs()
      throws Exception {
    assertE2eWriteReadLongs(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "DELTA", "DELTA", "LZ4")),
        generateMonotonicLongs(E2E_NUM_VALUES, 0L, 100L));
  }

  @Test
  public void testE2eRandomIntsWithPipeline()
      throws Exception {
    Random random = new Random(54321);
    int[] values = new int[E2E_NUM_VALUES];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextInt();
    }
    assertE2eWriteReadInts(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "ZSTANDARD")), values);
  }

  @Test
  public void testE2eRandomLongsWithPipeline()
      throws Exception {
    Random random = new Random(54321);
    long[] values = new long[E2E_NUM_VALUES];
    for (int i = 0; i < values.length; i++) {
      values[i] = random.nextLong();
    }
    assertE2eWriteReadLongs(
        ChunkCodecPipeline.fromNames(Arrays.asList("DELTA", "LZ4")), values);
  }

  @Test
  public void testE2ePipelineMetadata()
      throws Exception {
    ChunkCodecPipeline pipeline = ChunkCodecPipeline.fromNames(
        Arrays.asList("DELTA", "DOUBLE_DELTA", "ZSTANDARD"));
    int[] values = generateMonotonicInts(E2E_NUM_VALUES, 0, 1);

    File outFile = new File(E2E_TEST_FILE + "_metadata");
    FileUtils.deleteQuietly(outFile);
    try {
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(
          outFile, ChunkCompressionType.ZSTANDARD, pipeline, E2E_NUM_VALUES,
          E2E_NUM_DOCS_PER_CHUNK, Integer.BYTES, 7)) {
        for (int v : values) {
          writer.putInt(v);
        }
      }
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outFile);
          FixedBytePower2ChunkSVForwardIndexReader reader =
              new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.INT)) {
        // Verify the codec pipeline round-trips through the header
        ChunkCodecPipeline readPipeline = reader.getCodecPipeline();
        assertEquals(readPipeline, pipeline);
        assertEquals(readPipeline.getStages(),
            Arrays.asList(ChunkCodec.DELTA, ChunkCodec.DOUBLE_DELTA, ChunkCodec.ZSTANDARD));
        assertTrue(readPipeline.hasTransforms());
        assertEquals(readPipeline.getCompressor(), ChunkCodec.ZSTANDARD);
      }
    } finally {
      FileUtils.deleteQuietly(outFile);
    }
  }

  // ===== Helpers =====

  private void assertRoundTripInts(ChunkTransform transform, int[] original) {
    int numBytes = original.length * Integer.BYTES;
    ByteBuffer buffer = ByteBuffer.allocate(Math.max(numBytes, 1));
    for (int v : original) {
      buffer.putInt(v);
    }
    buffer.flip();

    transform.encode(buffer, numBytes, Integer.BYTES);
    transform.decode(buffer, numBytes, Integer.BYTES);

    buffer.position(0);
    for (int i = 0; i < original.length; i++) {
      assertEquals(buffer.getInt(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertRoundTripLongs(ChunkTransform transform, long[] original) {
    int numBytes = original.length * Long.BYTES;
    ByteBuffer buffer = ByteBuffer.allocate(Math.max(numBytes, 1));
    for (long v : original) {
      buffer.putLong(v);
    }
    buffer.flip();

    transform.encode(buffer, numBytes, Long.BYTES);
    transform.decode(buffer, numBytes, Long.BYTES);

    buffer.position(0);
    for (int i = 0; i < original.length; i++) {
      assertEquals(buffer.getLong(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertPipelineRoundTripInts(ChunkCodecPipeline pipeline, int[] original)
      throws IOException {
    int numBytes = original.length * Integer.BYTES;
    int valueSizeInBytes = Integer.BYTES;

    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(pipeline, valueSizeInBytes);
    ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(pipeline, valueSizeInBytes);

    // Fill input buffer
    ByteBuffer input = ByteBuffer.allocateDirect(numBytes);
    for (int v : original) {
      input.putInt(v);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(numBytes));
    compressor.compress(input, compressed);

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocateDirect(numBytes);
    decompressor.decompress(compressed, decompressed);

    // Verify — decompressor already flips the output buffer per Pinot convention
    for (int i = 0; i < original.length; i++) {
      assertEquals(decompressed.getInt(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertPipelineRoundTripLongs(ChunkCodecPipeline pipeline, long[] original)
      throws IOException {
    int numBytes = original.length * Long.BYTES;
    int valueSizeInBytes = Long.BYTES;

    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(pipeline, valueSizeInBytes);
    ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(pipeline, valueSizeInBytes);

    // Fill input buffer
    ByteBuffer input = ByteBuffer.allocateDirect(numBytes);
    for (long v : original) {
      input.putLong(v);
    }
    input.flip();

    // Compress
    ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(numBytes));
    compressor.compress(input, compressed);

    // Decompress
    ByteBuffer decompressed = ByteBuffer.allocateDirect(numBytes);
    decompressor.decompress(compressed, decompressed);

    // Verify — decompressor already flips the output buffer per Pinot convention
    for (int i = 0; i < original.length; i++) {
      assertEquals(decompressed.getLong(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertPipelineRoundTripFloats(ChunkCodecPipeline pipeline, float[] original)
      throws IOException {
    int numBytes = original.length * Float.BYTES;
    int valueSizeInBytes = Float.BYTES;

    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(pipeline, valueSizeInBytes);
    ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(pipeline, valueSizeInBytes);

    ByteBuffer input = ByteBuffer.allocateDirect(numBytes);
    for (float v : original) {
      input.putFloat(v);
    }
    input.flip();

    ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(numBytes));
    compressor.compress(input, compressed);

    ByteBuffer decompressed = ByteBuffer.allocateDirect(numBytes);
    decompressor.decompress(compressed, decompressed);

    for (int i = 0; i < original.length; i++) {
      assertEquals(decompressed.getFloat(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertPipelineRoundTripDoubles(ChunkCodecPipeline pipeline, double[] original)
      throws IOException {
    int numBytes = original.length * Double.BYTES;
    int valueSizeInBytes = Double.BYTES;

    ChunkCompressor compressor = ChunkCompressorFactory.getCompressor(pipeline, valueSizeInBytes);
    ChunkDecompressor decompressor = ChunkCompressorFactory.getDecompressor(pipeline, valueSizeInBytes);

    ByteBuffer input = ByteBuffer.allocateDirect(numBytes);
    for (double v : original) {
      input.putDouble(v);
    }
    input.flip();

    ByteBuffer compressed = ByteBuffer.allocateDirect(compressor.maxCompressedSize(numBytes));
    compressor.compress(input, compressed);

    ByteBuffer decompressed = ByteBuffer.allocateDirect(numBytes);
    decompressor.decompress(compressed, decompressed);

    for (int i = 0; i < original.length; i++) {
      assertEquals(decompressed.getDouble(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertChainedTransformRoundTripInts(ChunkTransform[] transforms, int[] original) {
    int numBytes = original.length * Integer.BYTES;
    ByteBuffer buffer = ByteBuffer.allocate(Math.max(numBytes, 1));
    for (int v : original) {
      buffer.putInt(v);
    }
    buffer.flip();

    // Encode: apply transforms left-to-right
    for (ChunkTransform t : transforms) {
      t.encode(buffer, numBytes, Integer.BYTES);
    }
    // Decode: apply transforms right-to-left
    for (int i = transforms.length - 1; i >= 0; i--) {
      transforms[i].decode(buffer, numBytes, Integer.BYTES);
    }

    buffer.position(0);
    for (int i = 0; i < original.length; i++) {
      assertEquals(buffer.getInt(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertChainedTransformRoundTripLongs(ChunkTransform[] transforms, long[] original) {
    int numBytes = original.length * Long.BYTES;
    ByteBuffer buffer = ByteBuffer.allocate(Math.max(numBytes, 1));
    for (long v : original) {
      buffer.putLong(v);
    }
    buffer.flip();

    for (ChunkTransform t : transforms) {
      t.encode(buffer, numBytes, Long.BYTES);
    }
    for (int i = transforms.length - 1; i >= 0; i--) {
      transforms[i].decode(buffer, numBytes, Long.BYTES);
    }

    buffer.position(0);
    for (int i = 0; i < original.length; i++) {
      assertEquals(buffer.getLong(), original[i], "Mismatch at index " + i);
    }
  }

  private void assertE2eWriteReadInts(ChunkCodecPipeline pipeline, int[] expected)
      throws Exception {
    File outFile = new File(E2E_TEST_FILE + "_int");
    FileUtils.deleteQuietly(outFile);
    try {
      // Write using version 7 writer with codec pipeline
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(
          outFile, pipeline.getChunkCompressionType(), pipeline, E2E_NUM_VALUES,
          E2E_NUM_DOCS_PER_CHUNK, Integer.BYTES, 7)) {
        for (int v : expected) {
          writer.putInt(v);
        }
      }

      // Read back via the standard reader and verify every value
      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outFile);
          ForwardIndexReader<ChunkReaderContext> reader =
              new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.INT);
          ChunkReaderContext context = reader.createContext()) {
        for (int i = 0; i < expected.length; i++) {
          assertEquals(reader.getInt(i, context), expected[i], "Mismatch at docId " + i);
        }
      }
    } finally {
      FileUtils.deleteQuietly(outFile);
    }
  }

  private void assertE2eWriteReadLongs(ChunkCodecPipeline pipeline, long[] expected)
      throws Exception {
    File outFile = new File(E2E_TEST_FILE + "_long");
    FileUtils.deleteQuietly(outFile);
    try {
      try (FixedByteChunkForwardIndexWriter writer = new FixedByteChunkForwardIndexWriter(
          outFile, pipeline.getChunkCompressionType(), pipeline, E2E_NUM_VALUES,
          E2E_NUM_DOCS_PER_CHUNK, Long.BYTES, 7)) {
        for (long v : expected) {
          writer.putLong(v);
        }
      }

      try (PinotDataBuffer buffer = PinotDataBuffer.mapReadOnlyBigEndianFile(outFile);
          ForwardIndexReader<ChunkReaderContext> reader =
              new FixedBytePower2ChunkSVForwardIndexReader(buffer, DataType.LONG);
          ChunkReaderContext context = reader.createContext()) {
        for (int i = 0; i < expected.length; i++) {
          assertEquals(reader.getLong(i, context), expected[i], "Mismatch at docId " + i);
        }
      }
    } finally {
      FileUtils.deleteQuietly(outFile);
    }
  }

  private static int[] generateMonotonicInts(int count, int start, int step) {
    int[] values = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = start + i * step;
    }
    return values;
  }

  private static long[] generateMonotonicLongs(int count, long start, long step) {
    long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = start + i * step;
    }
    return values;
  }
}
