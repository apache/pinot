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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.spi.memory.CleanerUtil;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Real-codec coverage of caller-owned destinations, independent of the forward-index format.
public class ChunkCodecHandlerTest {
  private enum SourceKind {
    HEAP, DIRECT, DIRECT_SLICE
  }

  @DataProvider(name = "codecs")
  public Object[][] codecs() {
    return new Object[][]{
        {Lz4CodecDefinition.INSTANCE, Lz4CodecDefinition.OPTIONS, DataType.BYTES},
        {SnappyCodecDefinition.INSTANCE, SnappyCodecDefinition.OPTIONS, DataType.BYTES},
        {GzipCodecDefinition.INSTANCE, GzipCodecDefinition.OPTIONS, DataType.BYTES},
        {ZstdCodecDefinition.INSTANCE, ZstdCodecDefinition.INSTANCE.parseOptions(List.of()), DataType.BYTES},
        {ZstdCodecDefinition.INSTANCE, ZstdCodecDefinition.INSTANCE.parseOptions(List.of("1")), DataType.BYTES},
        {DeltaCodecDefinition.INSTANCE, DeltaCodecDefinition.OPTIONS, DataType.INT},
        {DeltaCodecDefinition.INSTANCE, DeltaCodecDefinition.OPTIONS, DataType.LONG},
        {DeltaDeltaCodecDefinition.INSTANCE, DeltaDeltaCodecDefinition.OPTIONS, DataType.INT},
        {DeltaDeltaCodecDefinition.INSTANCE, DeltaDeltaCodecDefinition.OPTIONS, DataType.LONG},
        {T64CodecDefinition.INSTANCE, T64CodecDefinition.OPTIONS, DataType.INT},
        {T64CodecDefinition.INSTANCE, T64CodecDefinition.OPTIONS, DataType.LONG},
        {GorillaCodecDefinition.INSTANCE, GorillaCodecDefinition.OPTIONS, DataType.INT},
        {GorillaCodecDefinition.INSTANCE, GorillaCodecDefinition.OPTIONS, DataType.LONG}
    };
  }

  @Test(dataProvider = "codecs")
  public <O extends CodecOptions> void testCallerDestinationContract(ChunkCodecHandler<O> codec, O options,
      DataType dataType) throws IOException {
    CodecContext context = new CodecContext(dataType);
    byte[] input = input(dataType);
    int bound = codec.maxEncodedSize(options, context, input.length);
    for (SourceKind kind : SourceKind.values()) {
      ByteBuffer sourceOwner = kind == SourceKind.HEAP ? ByteBuffer.allocate(bound + input.length + 7)
          : ByteBuffer.allocateDirect(bound + input.length + 7);
      try (GuardedDestination encoded = new GuardedDestination(bound);
          GuardedDestination decoded = new GuardedDestination(input.length)) {
        // Reuse the same destinations for a full chunk, an empty chunk, and a single value.
        for (int length : new int[]{input.length, 0, dataType == DataType.BYTES ? 1 : dataType.size()}) {
          byte[] expected = Arrays.copyOf(input, length);
          ByteBuffer source = source(sourceOwner, ByteBuffer.wrap(expected), kind);
          encoded.dirty();
          codec.encode(options, context, source, encoded._view);
          assertEquals(encoded._view.position(), 0, codec.name());
          assertTrue(encoded._view.remaining() <= codec.maxEncodedSize(options, context, length), codec.name());
          encoded.assertGuards();

          source = source(sourceOwner, encoded._view, kind);
          decoded.dirty();
          codec.decode(options, context, source, decoded._view);
          assertBytes(decoded._view, expected);
          decoded.assertGuards();
        }
      } finally {
        CleanerUtil.cleanQuietly(sourceOwner);
      }
    }
  }

  @Test(dataProvider = "codecs")
  public <O extends CodecOptions> void testRejectsUndersizedDestinationsAndRecovers(ChunkCodecHandler<O> codec,
      O options, DataType dataType) throws IOException {
    CodecContext context = new CodecContext(dataType);
    byte[] input = input(dataType);
    try (GuardedDestination encoded = new GuardedDestination(codec.maxEncodedSize(options, context, input.length));
        GuardedDestination noSpace = new GuardedDestination(0);
        GuardedDestination shortDecoded = new GuardedDestination(input.length - 1);
        GuardedDestination decoded = new GuardedDestination(input.length)) {
      IllegalArgumentException encodeFailure = expectThrows(IllegalArgumentException.class,
          () -> codec.encode(options, context, ByteBuffer.wrap(input), noSpace._view));
      assertTrue(encodeFailure.getMessage().contains("capacity"), encodeFailure.getMessage());
      noSpace.assertGuards();
      codec.encode(options, context, ByteBuffer.wrap(input), encoded._view);

      IllegalArgumentException decodeFailure = expectThrows(IllegalArgumentException.class,
          () -> codec.decode(options, context, encoded._view.duplicate(), shortDecoded._view));
      assertTrue(decodeFailure.getMessage().contains("capacity"), decodeFailure.getMessage());
      shortDecoded.assertGuards();
      codec.decode(options, context, encoded._view.duplicate(), decoded._view);
      assertBytes(decoded._view, input);
      encoded.assertGuards();
      decoded.assertGuards();
    }
  }

  @Test(dataProvider = "codecs")
  public <O extends CodecOptions> void testHeapDestinationRequirements(ChunkCodecHandler<O> codec, O options,
      DataType dataType) throws IOException {
    CodecContext context = new CodecContext(dataType);
    byte[] input = input(dataType);
    int bound = codec.maxEncodedSize(options, context, input.length);
    ByteBuffer heapEncoded = ByteBuffer.allocate(bound);
    // Encoding and decoding have different requirements: LZ4 can decode into heap memory.
    boolean directEncode = codec instanceof Lz4CodecDefinition || codec instanceof SnappyCodecDefinition
        || codec instanceof ZstdCodecDefinition;
    if (directEncode) {
      IllegalArgumentException failure = expectThrows(IllegalArgumentException.class,
          () -> codec.encode(options, context, ByteBuffer.wrap(input), heapEncoded));
      assertTrue(failure.getMessage().contains("direct"), failure.getMessage());
    } else {
      codec.encode(options, context, ByteBuffer.wrap(input), heapEncoded);
      ByteBuffer heapDecoded = ByteBuffer.allocate(input.length);
      codec.decode(options, context, heapEncoded, heapDecoded);
      assertBytes(heapDecoded, input);
    }

    try (GuardedDestination encoded = new GuardedDestination(bound)) {
      codec.encode(options, context, ByteBuffer.wrap(input), encoded._view);
      ByteBuffer heapDecoded = ByteBuffer.allocate(input.length);
      // Native-only decoders require callers to supply direct buffers; the executor validates that precondition.
      if (!codec.requiresDirectDecodeDstBuffer()) {
        codec.decode(options, context, encoded._view, heapDecoded);
        assertBytes(heapDecoded, input);
      }
    }
  }

  private static byte[] input(DataType dataType) {
    if (dataType == DataType.BYTES) {
      byte[] input = new byte[257];
      for (int i = 0; i < input.length; i++) {
        input[i] = (byte) ((i * 31) ^ (i >>> 3));
      }
      return input;
    }
    ByteBuffer input = ByteBuffer.allocate(65 * dataType.size());
    long[] values = dataType == DataType.INT ? new long[]{Integer.MIN_VALUE, Integer.MAX_VALUE, -1, 0, 1}
        : new long[]{Long.MIN_VALUE, Long.MAX_VALUE, -1, 0, 1};
    for (int i = 0; i < 65; i++) {
      if (dataType == DataType.INT) {
        input.putInt((int) values[i % values.length]);
      } else {
        input.putLong(values[i % values.length]);
      }
    }
    return input.array();
  }

  private static ByteBuffer source(ByteBuffer owner, ByteBuffer bytes, SourceKind kind) {
    int offset = kind == SourceKind.DIRECT_SLICE ? 7 : 0;
    owner.clear().position(offset);
    owner.put(bytes.duplicate()).flip().position(offset);
    return (kind == SourceKind.DIRECT_SLICE ? owner.slice() : owner).order(ByteOrder.LITTLE_ENDIAN);
  }

  private static void assertBytes(ByteBuffer actual, byte[] expected) {
    assertEquals(actual.position(), 0);
    assertEquals(actual.limit(), expected.length);
    byte[] bytes = new byte[actual.remaining()];
    actual.duplicate().get(bytes);
    assertEquals(bytes, expected);
  }

  /// Owns the direct allocation; only the guarded slice is passed to the codec.
  private static final class GuardedDestination implements AutoCloseable {
    private static final int GUARD_SIZE = 7;
    private static final byte SENTINEL = (byte) 0xA5;
    private final ByteBuffer _owner;
    private final ByteBuffer _view;

    private GuardedDestination(int capacity) {
      _owner = ByteBuffer.allocateDirect(capacity + 2 * GUARD_SIZE);
      while (_owner.hasRemaining()) {
        _owner.put(SENTINEL);
      }
      ByteBuffer view = _owner.duplicate().clear();
      view.position(GUARD_SIZE).limit(GUARD_SIZE + capacity);
      _view = view.slice();
    }

    private void dirty() {
      _view.clear().limit(Math.min(1, _view.capacity()));
      _view.position(_view.limit());
    }

    private void assertGuards() {
      for (int i = 0; i < GUARD_SIZE; i++) {
        assertEquals(_owner.get(i), SENTINEL, "prefix guard");
        assertEquals(_owner.get(_owner.capacity() - 1 - i), SENTINEL, "suffix guard");
      }
    }

    @Override
    public void close() {
      CleanerUtil.cleanQuietly(_owner);
    }
  }
}
