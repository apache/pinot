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
import org.apache.pinot.segment.spi.codec.CodecOptions;


/// Transform codec that computes integer deltas between successive values before the
/// compression stage (if any).
///
/// DSL form: `DELTA` (no arguments)
///
/// Supported stored types: `INT`, `LONG`.
///
/// Wire format (header-less passthrough — the element type comes from the column context and the
/// value count from the buffer length, so the output is itself a same-width typed value array that
/// a following transform can consume):
/// ```
///   [element_size bytes: first value verbatim]
///   [(count-1) * element_size bytes: successive deltas]
/// ```
public final class DeltaCodecDefinition extends BaseDeltaCodecDefinition<DeltaCodecDefinition.Options> {

  /// On-disk permanent name stored verbatim in segment file headers.
  /// This string is a frozen on-disk API contract and must never be changed.
  public static final String NAME = "DELTA";

  public static final DeltaCodecDefinition INSTANCE = new DeltaCodecDefinition();

  private DeltaCodecDefinition() {
  }

  /// Singleton options object — DELTA has no configurable parameters.
  public static final class Options implements CodecOptions {
    public static final Options INSTANCE = new Options();

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
    return Options.INSTANCE;
  }

  @Override
  public String canonicalize(Options options) {
    return NAME;
  }

  @Override
  protected ByteBuffer encodeInt(ByteBuffer src, int count) {
    ByteBuffer out = ByteBuffer.allocateDirect(count * Integer.BYTES);
    if (count == 0) {
      out.flip();
      return out;
    }
    int prev = src.getInt();
    out.putInt(prev);
    for (int i = 1; i < count; i++) {
      int cur = src.getInt();
      out.putInt(cur - prev);
      prev = cur;
    }
    out.flip();
    return out;
  }

  @Override
  protected ByteBuffer encodeLong(ByteBuffer src, int count) {
    ByteBuffer out = ByteBuffer.allocateDirect(count * Long.BYTES);
    if (count == 0) {
      out.flip();
      return out;
    }
    long prev = src.getLong();
    out.putLong(prev);
    for (int i = 1; i < count; i++) {
      long cur = src.getLong();
      out.putLong(cur - prev);
      prev = cur;
    }
    out.flip();
    return out;
  }

  @Override
  protected ByteBuffer decodeInt(ByteBuffer src, int count) {
    ByteBuffer out = ByteBuffer.allocateDirect(count * Integer.BYTES);
    int prev = src.getInt();
    out.putInt(prev);
    for (int i = 1; i < count; i++) {
      int delta = src.getInt();
      prev += delta;
      out.putInt(prev);
    }
    out.flip();
    return out;
  }

  @Override
  protected ByteBuffer decodeLong(ByteBuffer src, int count) {
    ByteBuffer out = ByteBuffer.allocateDirect(count * Long.BYTES);
    long prev = src.getLong();
    out.putLong(prev);
    for (int i = 1; i < count; i++) {
      long delta = src.getLong();
      prev += delta;
      out.putLong(prev);
    }
    out.flip();
    return out;
  }

  @Override
  protected void decodeIntInto(ByteBuffer src, int count, ByteBuffer dst) {
    int prev = src.getInt();
    dst.putInt(prev);
    for (int i = 1; i < count; i++) {
      int delta = src.getInt();
      prev += delta;
      dst.putInt(prev);
    }
  }

  @Override
  protected void decodeLongInto(ByteBuffer src, int count, ByteBuffer dst) {
    long prev = src.getLong();
    dst.putLong(prev);
    for (int i = 1; i < count; i++) {
      long delta = src.getLong();
      prev += delta;
      dst.putLong(prev);
    }
  }
}
