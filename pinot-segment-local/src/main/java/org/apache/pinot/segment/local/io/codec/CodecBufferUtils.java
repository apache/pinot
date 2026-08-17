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
import org.apache.pinot.segment.spi.memory.CleanerUtil;


/// Package-private buffer helpers shared across codec handler implementations.
final class CodecBufferUtils {

  private CodecBufferUtils() {
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
