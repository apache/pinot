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


/// Extension of [CodecDefinition] that adds the encode/decode operations needed to execute a
/// codec pipeline over forward-index chunks.
///
/// All implementations are expected to be stateless and thread-safe.
///
/// Buffer contract for [#encode()] and [#decode()]:
///
/// - `src` supplies the readable range from its current position to its limit. Typed codecs
///       interpret values in persisted `BIG_ENDIAN` order. Implementations may advance its
///       position or change its byte order; callers that need to reuse it must preserve its state
///       or pass a duplicate.
/// - `dst` is writable, uses `BIG_ENDIAN` byte order, and does not overlap `src`. Implementations
///       clear it before writing and leave it ready for read (position=0, limit=output bytes) on
///       successful return. Its position and limit are unspecified after failure.
/// - Both buffers remain caller-owned: implementations must not retain or release them. The
///       destination capacity bounds the output; callers requiring a tighter bound must pass a
///       capacity-limited view, because clearing the buffer discards its previous limit.
/// - Heap input is supported, but JNI codecs may copy it into a temporary direct buffer. Use
///       direct input on hot paths to avoid that allocation.
///
/// @param <O> typed [CodecOptions] for this codec
interface ChunkCodecHandler<O extends CodecOptions> extends CodecDefinition<O> {

  /// Encodes `src` into caller-owned `dst`, without allocating an output buffer.
  /// Encoding callers must supply a direct destination to support all registered codecs.
  ///
  /// @param options parsed options for this codec invocation
  /// @param ctx     column context
  /// @param src     unencoded data, ready for read
  /// @param dst     direct output buffer with at least [#maxEncodedSize(CodecOptions, CodecContext, int)]
  ///                bytes of capacity
  void encode(O options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException;

  /// Decodes `src` into caller-owned `dst`, without allocating an output buffer.
  ///
  /// Callers must ensure `dst` is a direct [ByteBuffer] when
  /// [#requiresDirectDecodeDstBuffer()] returns `true`.
  ///
  /// @param options parsed options for this codec invocation
  /// @param ctx     column context
  /// @param src     encoded data, ready for read
  /// @param dst     output buffer; must be direct when required; must have sufficient capacity
  void decode(O options, CodecContext ctx, ByteBuffer src, ByteBuffer dst) throws IOException;

  /// Returns an upper bound on the encoded byte count for an input of `inputSize` bytes.
  ///
  /// @param options   parsed options for this codec invocation
  /// @param ctx       column context
  /// @param inputSize unencoded input size in bytes
  int maxEncodedSize(O options, CodecContext ctx, int inputSize);

  /// Returns `true` if [#decode()] requires `dst` to be a direct
  /// [ByteBuffer] (e.g. codecs that delegate to JNI libraries with direct-buffer-only APIs).
  /// This requirement applies only to decoding; encoding callers always supply a direct buffer.
  boolean requiresDirectDecodeDstBuffer();
}
