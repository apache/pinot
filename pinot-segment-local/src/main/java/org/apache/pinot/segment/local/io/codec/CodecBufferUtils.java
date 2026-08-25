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
import org.apache.pinot.segment.spi.memory.CleanerUtil;


/// Package-private buffer helpers shared across codec handler implementations.
final class CodecBufferUtils {

  private CodecBufferUtils() {
  }

  /// Sanity cap (1 GiB) on any decompressed size declared by untrusted encoded segment data. A
  /// corrupt or hostile declaration must never drive a giant pre-allocation; 1 GiB is well above
  /// any realistic chunk size.
  static final long MAX_DECLARED_DECOMPRESSED_SIZE = 1L << 30;

  /// Validates a decompressed size declared by untrusted segment data and returns it when in range.
  ///
  /// Shared by every codec definition so the bound cannot be omitted from a new codec or silently
  /// drift between implementations.
  ///
  /// @param declared size read from the encoded data
  /// @param codec codec display name for the error message
  /// @param source where the size was read from, e.g. "length prefix" or "frame header"
  /// @return `declared`, guaranteed to be in `[0, MAX_DECLARED_DECOMPRESSED_SIZE]`
  /// @throws IOException if the declared size is negative or exceeds the cap
  static int checkDeclaredDecompressedSize(long declared, String codec, String source)
      throws IOException {
    if (declared < 0 || declared > MAX_DECLARED_DECOMPRESSED_SIZE) {
      throw new IOException(codec + ": declared decompressed size " + declared + " in " + source
          + " is out of range [0, " + MAX_DECLARED_DECOMPRESSED_SIZE + "]. Segment may be corrupt.");
    }
    return (int) declared;
  }

  /// Returns `buf` if already direct; otherwise copies into a new direct buffer.
  ///
  /// **Hot-path note:** pipeline callers should pass direct buffers. The heap-copy branch is a
  /// defensive fallback for tests and ad-hoc callers; using it on production hot paths would add
  /// a per-call direct-buffer allocation.
  static ByteBuffer toDirectBuffer(ByteBuffer buf) {
    if (buf.isDirect()) {
      return buf;
    }
    ByteBuffer direct = ByteBuffer.allocateDirect(buf.remaining());
    direct.put(buf.duplicate());
    direct.flip();
    return direct;
  }

  /// Releases `converted` when [#toDirectBuffer(ByteBuffer)] had to copy a heap buffer.
  static void cleanDirectCopy(ByteBuffer original, ByteBuffer converted) {
    if (converted != original) {
      CleanerUtil.cleanQuietly(converted);
    }
  }

  /// Releases an owned direct buffer after a failed codec operation.
  static void cleanQuietly(ByteBuffer buffer) {
    CleanerUtil.cleanQuietly(buffer);
  }
}
