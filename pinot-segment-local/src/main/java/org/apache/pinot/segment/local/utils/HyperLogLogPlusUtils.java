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
package org.apache.pinot.segment.local.utils;

import com.clearspring.analytics.stream.cardinality.RegisterSet;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;


public class HyperLogLogPlusUtils {
  private HyperLogLogPlusUtils() {
  }

  /// Maximum supported `sp`. The sparse representation packs the `sp`-bit sparse index, a 6-bit run length and a flag
  /// bit into an `int`, so a larger `sp` overflows and corrupts the registers once the sketch converts to the normal
  /// representation, even though stream-lib accepts `sp` up to 32.
  public static final int MAX_SP = 25;

  private static final int MAX_VAR_INT_BYTES = 5;

  /// Validates that `sp` does not exceed [#MAX_SP].
  public static void validateSp(int sp) {
    Preconditions.checkArgument(sp <= MAX_SP, "HyperLogLogPlus sp must be at most %s, got: %s", MAX_SP, sp);
  }

  /// Returns the maximum serialized size of a `HyperLogLogPlus` with the given `p` and `sp`: an upper bound of
  /// `HyperLogLogPlus.getBytes().length` over every sketch with these parameters, not the length of any one of them.
  ///
  /// The serialized form is a 4-byte version, varints of `p`, `sp` and the format flag, then the format payload:
  /// - Normal format: a varint of the register set byte size, then the register set. Its size depends only on `p`, so
  ///   with `sp == 0` (sparse representation disabled) every sketch serializes to exactly the returned size.
  /// - Sparse format (`sp > 0`, until the sketch converts to normal format): a varint of the sparse set length, then a
  ///   delta varint of up to 5 bytes per entry. The sketch converts to normal format once the sparse set exceeds
  ///   `0.75 * (1 << p)` entries, which bounds the entries a sparse sketch can serialize.
  public static int maxByteSize(int p, int sp) {
    int headerByteSize = Integer.BYTES + varIntSize(p) + varIntSize(sp) + 1;
    int registerSetByteSize = RegisterSet.getSizeForCount(1 << p) * Integer.BYTES;
    int normalByteSize = headerByteSize + varIntSize(registerSetByteSize) + registerSetByteSize;
    if (sp == 0) {
      return normalByteSize;
    }
    int sparseSetThreshold = (int) ((1 << p) * 0.75);
    int sparseByteSize = headerByteSize + varIntSize(sparseSetThreshold) + sparseSetThreshold * MAX_VAR_INT_BYTES;
    return Math.max(normalByteSize, sparseByteSize);
  }

  /// Returns the [#maxByteSize(int, int)] for the `p` and `sp` of the given serialized `HyperLogLogPlus`, read from its
  /// header. Both the current format (a negative version followed by varints of `p` and `sp`) and the legacy format
  /// without version (ints of `p` and `sp`) are supported. In the current format, each varint is a single byte because
  /// `p` and `sp` are below 128: stream-lib rejects `sp` above 32, and `p` above 30 overflows the register count.
  public static int maxByteSize(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int version = buffer.getInt();
    if (version < 0) {
      int p = buffer.get();
      int sp = buffer.get();
      return maxByteSize(p, sp);
    } else {
      return maxByteSize(version, buffer.getInt());
    }
  }

  /// Returns the number of bytes in the unsigned varint encoding of the given value.
  private static int varIntSize(int value) {
    return (Integer.SIZE - 1 - Integer.numberOfLeadingZeros(value | 1)) / 7 + 1;
  }
}
