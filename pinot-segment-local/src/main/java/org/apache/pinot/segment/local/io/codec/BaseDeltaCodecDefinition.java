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
import org.apache.pinot.segment.spi.codec.ChunkCodecHandler;
import org.apache.pinot.segment.spi.codec.CodecContext;
import org.apache.pinot.segment.spi.codec.CodecKind;
import org.apache.pinot.segment.spi.codec.CodecOptions;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/// Shared structure for [DeltaCodecDefinition] and [DeltaDeltaCodecDefinition].
///
/// **Value-preserving passthrough.** DELTA/DELTADELTA map an array of `count` column-typed values
/// to an array of the same `count` values of the same width (no per-frame header). The element
/// type comes from the [CodecContext] and `count` is derived from the buffer length, so the output
/// is itself a contiguous typed value array that a following [CodecKind#TRANSFORM] stage (or the
/// chunk reader) can consume directly. This is what makes chains like `CODEC(DELTA, DELTADELTA, LZ4)`
/// and `CODEC(DELTA, T64, LZ4)` work — see [#isValuePreserving()].
///
/// **Two's-complement wrap-around is intentional.** Deltas are computed using ordinary Java
/// `-` on int/long, which silently wraps when the mathematical delta does not fit in the
/// value's width (e.g. `Integer.MAX_VALUE - Integer.MIN_VALUE`). Decode applies the symmetric
/// `+` so the original sequence is recovered bit-for-bit. Boundary tests in
/// `CodecPipelineForwardIndexTest` lock in this property — do not switch the encode/decode
/// helpers to checked arithmetic (`Math.subtractExact`, `Math.addExact`) without also
/// changing the on-disk format.
abstract class BaseDeltaCodecDefinition<O extends CodecOptions> implements ChunkCodecHandler<O> {

  @Override
  public final CodecKind kind() {
    return CodecKind.TRANSFORM;
  }

  @Override
  public final boolean isValuePreserving() {
    // Output is a same-width typed value array, so DELTA/DELTADELTA can be chained ahead of another
    // transform (or each other).
    return true;
  }

  @Override
  public final void validateContext(O options, CodecContext ctx) {
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException(
          name() + " codec only supports INT and LONG columns, but column has type: " + dt);
    }
  }

  @Override
  public final int maxEncodedSize(O options, int inputSize) {
    // Passthrough: output is exactly the same size as the input (no header).
    return inputSize;
  }

  @Override
  public final boolean requiresDirectDstBuffer() {
    return false;
  }

  /// Subclasses must reject non-empty `args`.
  @Override
  public abstract O parseOptions(List<String> args);

  @Override
  public final ByteBuffer encode(O options, CodecContext ctx, ByteBuffer src) {
    int elementSize = elementSize(ctx, src.remaining());
    int count = src.remaining() / elementSize;
    return elementSize == Long.BYTES ? encodeLong(src, count) : encodeInt(src, count);
  }

  @Override
  public final ByteBuffer decode(O options, CodecContext ctx, ByteBuffer src) {
    int elementSize = elementSize(ctx, src.remaining());
    int count = src.remaining() / elementSize;
    if (count == 0) {
      return ByteBuffer.allocateDirect(0);
    }
    return elementSize == Long.BYTES ? decodeLong(src, count) : decodeInt(src, count);
  }

  @Override
  public final void decodeInto(O options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) {
    dst.clear();
    int elementSize = elementSize(ctx, src.remaining());
    int count = src.remaining() / elementSize;
    long requiredCapacity = (long) count * elementSize;
    if (requiredCapacity > dst.capacity()) {
      throw new IllegalArgumentException(
          name() + ": decoded size " + requiredCapacity + " exceeds dst capacity " + dst.capacity());
    }
    if (count > 0) {
      if (elementSize == Long.BYTES) {
        decodeLongInto(src, count, dst);
      } else {
        decodeIntInto(src, count, dst);
      }
    }
    dst.flip();
  }

  /// Resolves the element size from the column context and validates that the buffer holds a whole
  /// number of values. The element type (INT vs LONG) is carried by the pipeline context rather
  /// than a per-frame flag, which is what allows these transforms to be chained.
  private int elementSize(CodecContext ctx, int remaining) {
    DataType dt = ctx.getDataType();
    if (dt != DataType.INT && dt != DataType.LONG) {
      throw new IllegalArgumentException(name() + " only supports INT and LONG columns, but column has type: " + dt);
    }
    int elementSize = dt.size();
    if (remaining % elementSize != 0) {
      throw new IllegalArgumentException(
          name() + ": buffer size (" + remaining + ") is not a multiple of element size (" + elementSize
              + ") for stored type " + dt);
    }
    return elementSize;
  }

  // -------------------------------------------------------------------------
  // Arithmetic helpers — subclasses provide the encoding/decoding logic.
  // All operate on a header-less, same-width array of `count` values.
  // -------------------------------------------------------------------------

  protected abstract ByteBuffer encodeInt(ByteBuffer src, int count);

  protected abstract ByteBuffer encodeLong(ByteBuffer src, int count);

  protected abstract ByteBuffer decodeInt(ByteBuffer src, int count);

  protected abstract ByteBuffer decodeLong(ByteBuffer src, int count);

  protected abstract void decodeIntInto(ByteBuffer src, int count, ByteBuffer dst);

  protected abstract void decodeLongInto(ByteBuffer src, int count, ByteBuffer dst);
}
