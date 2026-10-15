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


/// Transform codec that computes integer deltas between successive values before the
/// compression stage (if any).
///
/// DSL form: `DELTA` (no arguments)
///
/// Supported stored types: `INT`, `LONG`.
///
/// Stateless and thread-safe; the [#INSTANCE] singleton is shared across all columns.
///
/// Wire format (header-less passthrough — the element type comes from the column context and the
/// value count from the buffer length, so the output is itself a same-width typed value array that
/// a following transform can consume):
/// ```
///   [element_size bytes: first value verbatim]
///   [(count-1) * element_size bytes: successive deltas]
/// ```
final class DeltaCodecDefinition extends BaseDeltaCodecDefinition<DeltaCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "DELTA";

  public static final DeltaCodecDefinition INSTANCE = new DeltaCodecDefinition();

  /// Singleton options — DELTA has no configurable parameters.
  public static final Options OPTIONS = new Options();

  private DeltaCodecDefinition() {
  }

  /// Typed options for [DeltaCodecDefinition]. DELTA has no configurable parameters.
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
      throw new IllegalArgumentException("DELTA codec takes no arguments but got: " + args);
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
    for (int i = 1; i < count; i++) {
      int cur = src.getInt(srcOffset + i * Integer.BYTES);
      dst.putInt(dstOffset + i * Integer.BYTES, cur - prev);
      prev = cur;
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
    for (int i = 1; i < count; i++) {
      long cur = src.getLong(srcOffset + i * Long.BYTES);
      dst.putLong(dstOffset + i * Long.BYTES, cur - prev);
      prev = cur;
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
    for (int i = 1; i < count; i++) {
      int delta = src.getInt(srcOffset + i * Integer.BYTES);
      prev += delta;
      dst.putInt(dstOffset + i * Integer.BYTES, prev);
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
    for (int i = 1; i < count; i++) {
      long delta = src.getLong(srcOffset + i * Long.BYTES);
      prev += delta;
      dst.putLong(dstOffset + i * Long.BYTES, prev);
    }
    src.position(srcOffset + count * Long.BYTES);
    dst.position(dstOffset + count * Long.BYTES);
  }
}
