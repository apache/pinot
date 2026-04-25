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


/// Package-private buffer helpers shared across codec handler implementations.
final class CodecBufferUtils {

  private CodecBufferUtils() {
  }

  /// Returns `buf` if already direct; otherwise copies into a new direct buffer.
  ///
  /// **Hot-path note:** all production callers (V7 writer's `_chunkBuffer`, V7 reader's
  /// `_dataBuffer.toDirectByteBuffer(...)`) pass direct buffers, so the heap-copy branch is a
  /// defensive fallback for tests and ad-hoc callers only. Do not introduce production paths that
  /// pass heap buffers — the per-call `allocateDirect` would be a hidden hot-path allocation.
  static ByteBuffer toDirectBuffer(ByteBuffer buf) {
    if (buf.isDirect()) {
      return buf;
    }
    ByteBuffer direct = ByteBuffer.allocateDirect(buf.remaining());
    direct.put(buf.duplicate());
    direct.flip();
    return direct;
  }

  /// Copies the readable bytes of `buf` into a new heap byte array.
  static byte[] toHeapArray(ByteBuffer buf) {
    byte[] bytes = new byte[buf.remaining()];
    buf.duplicate().get(bytes);
    return bytes;
  }
}
