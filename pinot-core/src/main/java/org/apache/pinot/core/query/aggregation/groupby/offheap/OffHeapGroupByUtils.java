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
package org.apache.pinot.core.query.aggregation.groupby.offheap;

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;


/// Static helpers for encoding group-by keys into the scratch byte buffers fed to [OffHeapBytesGroupIdMap].
/// All methods are allocation-free on their fast paths; callers own (and reuse) the scratch arrays.
///
/// Thread-safety: stateless; the scratch arrays passed in are owned by the single-threaded caller.
public final class OffHeapGroupByUtils {
  private OffHeapGroupByUtils() {
  }

  // Buffers larger than this get no direct view (ByteBuffer is int-indexed); test hook so the wrapper-based
  // fallback arms of every view fast path can be exercised without allocating multi-GB buffers
  private static volatile long _viewSizeLimitBytes = Integer.MAX_VALUE;

  @VisibleForTesting
  public static void setViewSizeLimitBytes(long viewSizeLimitBytes) {
    _viewSizeLimitBytes = viewSizeLimitBytes;
  }

  /// Returns an absolute-indexed native-order direct [ByteBuffer] view of the buffer for hot-path access
  /// (monomorphic, intrinsified access instead of the [PinotDataBuffer] wrapper), or null when the buffer is
  /// empty or exceeds the 2GB view limit — callers must fall back to the wrapper accessors then.
  public static ByteBuffer createView(PinotDataBuffer buffer, long sizeBytes) {
    return sizeBytes > 0 && sizeBytes <= _viewSizeLimitBytes
        ? buffer.toDirectByteBuffer(0, (int) sizeBytes, ByteOrder.nativeOrder()) : null;
  }

  /// Returns a scratch array of at least `capacity` bytes, growing (with doubling) if needed. Contents are not
  /// preserved on growth.
  public static byte[] ensureByteCapacity(byte[] scratch, int capacity) {
    if (scratch.length >= capacity) {
      return scratch;
    }
    return new byte[Math.max(capacity, scratch.length << 1)];
  }

  /// Encodes the given string into the scratch buffer as standard UTF-8 and returns the encoded length. The scratch
  /// buffer must have capacity of at least `3 * value.length()` bytes (a surrogate pair encodes 2 chars into 4
  /// bytes, so the bound holds for all inputs).
  ///
  /// This produces byte-for-byte the same encoding as `String.getBytes(StandardCharsets.UTF_8)` for all
  /// inputs, including supplementary characters (4-byte sequences) and malformed surrogates, which the JDK encoder
  /// replaces with `'?'` — pinned by `OffHeapGroupByUtilsTest`.
  public static int encodeUtf8(String value, byte[] scratch) {
    int length = value.length();
    int outIndex = 0;
    int charIndex = 0;
    while (charIndex < length) {
      char c = value.charAt(charIndex++);
      if (c < 0x80) {
        scratch[outIndex++] = (byte) c;
      } else if (c < 0x800) {
        scratch[outIndex++] = (byte) (0xC0 | (c >> 6));
        scratch[outIndex++] = (byte) (0x80 | (c & 0x3F));
      } else if (c >= Character.MIN_SURROGATE && c <= Character.MAX_SURROGATE) {
        if (Character.isHighSurrogate(c) && charIndex < length && Character.isLowSurrogate(value.charAt(charIndex))) {
          int codePoint = Character.toCodePoint(c, value.charAt(charIndex++));
          scratch[outIndex++] = (byte) (0xF0 | (codePoint >> 18));
          scratch[outIndex++] = (byte) (0x80 | ((codePoint >> 12) & 0x3F));
          scratch[outIndex++] = (byte) (0x80 | ((codePoint >> 6) & 0x3F));
          scratch[outIndex++] = (byte) (0x80 | (codePoint & 0x3F));
        } else {
          // Unpaired surrogate: the JDK UTF-8 encoder replaces it with '?'
          scratch[outIndex++] = '?';
        }
      } else {
        scratch[outIndex++] = (byte) (0xE0 | (c >> 12));
        scratch[outIndex++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        scratch[outIndex++] = (byte) (0x80 | (c & 0x3F));
      }
    }
    return outIndex;
  }

  /// Packs `numValues` ints into the scratch buffer (4 bytes each, big-endian) and returns the packed length.
  /// The scratch buffer must have capacity of at least `4 * numValues` bytes.
  public static int packInts(int[] values, int numValues, byte[] scratch) {
    int outIndex = 0;
    for (int i = 0; i < numValues; i++) {
      int value = values[i];
      scratch[outIndex++] = (byte) (value >> 24);
      scratch[outIndex++] = (byte) (value >> 16);
      scratch[outIndex++] = (byte) (value >> 8);
      scratch[outIndex++] = (byte) value;
    }
    return outIndex;
  }

  /// Unpacks `numValues` big-endian ints from the scratch buffer written by [#packInts].
  public static void unpackInts(byte[] scratch, int numValues, int[] dest) {
    int inIndex = 0;
    for (int i = 0; i < numValues; i++) {
      dest[i] = ((scratch[inIndex] & 0xFF) << 24) | ((scratch[inIndex + 1] & 0xFF) << 16)
          | ((scratch[inIndex + 2] & 0xFF) << 8) | (scratch[inIndex + 3] & 0xFF);
      inIndex += 4;
    }
  }
}
