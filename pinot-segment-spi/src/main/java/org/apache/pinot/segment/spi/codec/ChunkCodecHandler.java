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

import java.io.IOException;
import java.nio.ByteBuffer;


/// Extension of [CodecDefinition] that adds the encode/decode operations needed to execute a
/// codec pipeline over forward-index chunks.
///
/// All implementations are expected to be stateless and thread-safe.
///
/// Buffer contract for all methods:
///
/// - [#encode()] and [#decode()]: `src` is ready for read (position=0);
///       the returned buffer is ready for read and owned by the caller.
/// - [#decodeInto()]: implementations must treat `dst` as freshly cleared
///       (calling `dst.clear()` internally is recommended as a defensive first step);
///       `dst` is flipped (position=0, limit=decoded bytes) on return.
///
/// @param <O> typed [CodecOptions] for this codec
public interface ChunkCodecHandler<O extends CodecOptions> extends CodecDefinition<O> {

  /// Encodes `src` and returns the encoded bytes ready for read.
  ///
  /// **Position contract:** implementations may consume `src` (advance its position).
  /// Callers that need to re-read `src` after this call must pass `src.duplicate()`.
  ///
  /// @param options parsed options for this codec invocation
  /// @param ctx     column context (data type, etc.)
  /// @param src     unencoded data, ready for read
  /// @return encoded buffer ready for read; caller owns this buffer
  ByteBuffer encode(O options, CodecContext ctx, ByteBuffer src) throws IOException;

  /// Decodes `src` and returns the decoded bytes ready for read.
  ///
  /// **Position contract:** implementations may consume `src` (advance its position).
  /// Callers that need to re-read `src` after this call must pass `src.duplicate()`.
  ///
  /// @param options parsed options for this codec invocation
  /// @param ctx     column context
  /// @param src     encoded data, ready for read
  /// @return decoded buffer ready for read; caller owns this buffer
  ByteBuffer decode(O options, CodecContext ctx, ByteBuffer src) throws IOException;

  /// Decodes `src` directly into `dst`, avoiding an extra allocation.
  /// Implementations must treat `dst` as freshly cleared and flip it before returning.
  ///
  /// Callers must ensure `dst` is a direct [ByteBuffer] when
  /// [#requiresDirectDstBuffer()] returns `true`.
  ///
  /// @param options parsed options for this codec invocation
  /// @param ctx     column context
  /// @param src     encoded data, ready for read
  /// @param dst     output buffer; must be direct when required; must have sufficient capacity
  void decodeInto(O options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException;

  /// Returns an upper bound on the encoded byte count for an input of `inputSize` bytes.
  int maxEncodedSize(O options, int inputSize);

  /// Returns `true` if [#decodeInto()] requires `dst` to be a direct
  /// [ByteBuffer] (e.g. codecs that delegate to JNI libraries with direct-buffer-only APIs).
  boolean requiresDirectDstBuffer();
}
