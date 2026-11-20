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


/// Transform codec that computes delta-of-delta values between successive values before the
/// compression stage (if any). Useful for data where the differences between consecutive values
/// are approximately constant (e.g. timestamps with regular intervals).
///
/// DSL form: `DELTADELTA` (no arguments)
///
/// Supported stored types: `INT`, `LONG`.
///
/// Stateless and thread-safe; the [#INSTANCE] singleton is shared across all columns.
///
/// Wire format (header-less passthrough — element type from the column context, value count from
/// the buffer length, so the output is a same-width typed value array a following transform can
/// consume):
/// ```
///   [element_size bytes: first value verbatim]
///   [element_size bytes: first delta (second - first), if count > 1]
///   [(count-2) * element_size bytes: delta-of-deltas, if count > 2]
/// ```
final class DeltaDeltaCodecDefinition
    extends BaseDeltaCodecDefinition<DeltaDeltaCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "DELTADELTA";

  public static final DeltaDeltaCodecDefinition INSTANCE = new DeltaDeltaCodecDefinition();

  /// Singleton options — DELTADELTA has no configurable parameters.
  public static final Options OPTIONS = new Options();

  private DeltaDeltaCodecDefinition() {
  }

  /// Typed options for [DeltaDeltaCodecDefinition]. DELTADELTA has no configurable parameters.
  public static final class Options implements CodecOptions {
    private Options() {
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Options parseOptions(List<String> args) {
    if (!args.isEmpty()) {
      throw new IllegalArgumentException("DELTADELTA codec takes no arguments but got: " + args);
    }
    return OPTIONS;
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  protected void encodeIntInto(ByteBuffer src, int count, ByteBuffer dst) {
    int srcOffset = src.position();
    int dstOffset = dst.position();
    int prev = src.getInt(srcOffset);
    dst.putInt(dstOffset, prev);
    if (count > 1) {
      int prevDelta = src.getInt(srcOffset + Integer.BYTES) - prev;
      dst.putInt(dstOffset + Integer.BYTES, prevDelta);
      prev += prevDelta;
      for (int i = 2; i < count; i++) {
        int cur = src.getInt(srcOffset + i * Integer.BYTES);
        int curDelta = cur - prev;
        dst.putInt(dstOffset + i * Integer.BYTES, curDelta - prevDelta);
        prev = cur;
        prevDelta = curDelta;
      }
    }
    src.position(srcOffset + count * Integer.BYTES);
    dst.position(dstOffset + count * Integer.BYTES);
  }

  @Override
  protected void encodeLongInto(ByteBuffer src, int count, ByteBuffer dst) {
    int srcOffset = src.position();
    int dstOffset = dst.position();
    long prev = src.getLong(srcOffset);
    dst.putLong(dstOffset, prev);
    if (count > 1) {
      long prevDelta = src.getLong(srcOffset + Long.BYTES) - prev;
      dst.putLong(dstOffset + Long.BYTES, prevDelta);
      prev += prevDelta;
      for (int i = 2; i < count; i++) {
        long cur = src.getLong(srcOffset + i * Long.BYTES);
        long curDelta = cur - prev;
        dst.putLong(dstOffset + i * Long.BYTES, curDelta - prevDelta);
        prev = cur;
        prevDelta = curDelta;
      }
    }
    src.position(srcOffset + count * Long.BYTES);
    dst.position(dstOffset + count * Long.BYTES);
  }

  @Override
  protected void decodeIntInto(ByteBuffer src, int count, ByteBuffer dst) {
    int srcOffset = src.position();
    int dstOffset = dst.position();
    int prev = src.getInt(srcOffset);
    dst.putInt(dstOffset, prev);
    if (count > 1) {
      int prevDelta = src.getInt(srcOffset + Integer.BYTES);
      prev += prevDelta;
      dst.putInt(dstOffset + Integer.BYTES, prev);
      for (int i = 2; i < count; i++) {
        int dod = src.getInt(srcOffset + i * Integer.BYTES);
        prevDelta += dod;
        prev += prevDelta;
        dst.putInt(dstOffset + i * Integer.BYTES, prev);
      }
    }
    src.position(srcOffset + count * Integer.BYTES);
    dst.position(dstOffset + count * Integer.BYTES);
  }

  @Override
  protected void decodeLongInto(ByteBuffer src, int count, ByteBuffer dst) {
    int srcOffset = src.position();
    int dstOffset = dst.position();
    long prev = src.getLong(srcOffset);
    dst.putLong(dstOffset, prev);
    if (count > 1) {
      long prevDelta = src.getLong(srcOffset + Long.BYTES);
      prev += prevDelta;
      dst.putLong(dstOffset + Long.BYTES, prev);
      for (int i = 2; i < count; i++) {
        long dod = src.getLong(srcOffset + i * Long.BYTES);
        prevDelta += dod;
        prev += prevDelta;
        dst.putLong(dstOffset + i * Long.BYTES, prev);
      }
    }
    src.position(srcOffset + count * Long.BYTES);
    dst.position(dstOffset + count * Long.BYTES);
  }
}
