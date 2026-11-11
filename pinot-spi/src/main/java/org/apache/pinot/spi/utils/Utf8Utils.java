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
package org.apache.pinot.spi.utils;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;


/// UTF-8 helpers whose sizing and encoding agree: [#encodedLength] returns exactly the length of the array that
/// [#encode] produces for the same string.
public class Utf8Utils {
  private Utf8Utils() {
  }

  // Byte order is irrelevant: isAscii() only ORs the bytes together
  private static final VarHandle LONG_VIEW =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

  public static byte[] encode(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /// Returns the number of bytes in the UTF-8 encoding of `s`, which is exactly `encode(s).length`. An unpaired
  /// surrogate counts as the single replacement byte that [String#getBytes] writes for it, so this never throws.
  public static int encodedLength(String s) {
    int utf16Length = s.length();
    // Fast path for the common all-ASCII prefix, one byte per char
    int i = 0;
    while (i < utf16Length && s.charAt(i) < 0x80) {
      i++;
    }
    // Every char counts one byte up front. Add 1 for a 2-byte char (branch-free: (0x7f - c) >>> 31 is 1 iff c >= 0x80),
    // 2 for a 3-byte char or a surrogate pair, and nothing for an unpaired surrogate, which encodes as the single
    // replacement byte '?'. The surrogate ranges are compared inline instead of through Character.isSurrogate() and
    // similar helpers: C2 folds adjacent range checks that guard uncommon traps into one unsigned comparison only when
    // both sit in the same method, so a check inside an inlined helper stays a separate compare-and-branch per char,
    // which measured 1.6x slower on CJK text.
    int utf8Length = utf16Length;
    for (; i < utf16Length; i++) {
      char c = s.charAt(i);
      if (c < 0x800) {
        utf8Length += (0x7f - c) >>> 31;
      } else if (c < Character.MIN_SURROGATE || c > Character.MAX_SURROGATE) {
        utf8Length += 2;
      } else if (c <= Character.MAX_HIGH_SURROGATE && i + 1 < utf16Length) {
        char next = s.charAt(i + 1);
        if (next >= Character.MIN_LOW_SURROGATE && next <= Character.MAX_LOW_SURROGATE) {
          utf8Length += 2;
          i++;
        }
      }
    }
    return utf8Length;
  }

  public static String decode(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static boolean isAscii(byte[] bytes) {
    int length = bytes.length;
    int loopBound = length & ~0x7;
    long or = 0;
    int i = 0;
    for (; i < loopBound; i += 8) {
      or |= (long) LONG_VIEW.get(bytes, i);
    }
    for (; i < length; i++) {
      or |= bytes[i];
    }
    // Do not check within the loops because most values are ASCII
    return (or & 0x8080808080808080L) == 0;
  }
}
