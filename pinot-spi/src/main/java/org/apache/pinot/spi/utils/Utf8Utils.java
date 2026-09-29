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

import com.google.common.base.Utf8;
import java.nio.charset.StandardCharsets;


public class Utf8Utils {
  private Utf8Utils() {
  }

  public static byte[] encode(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  public static int encodedLength(String s) {
    return Utf8.encodedLength(s);
  }

  /// Returns the UTF-8 byte length using the same malformed UTF-16 replacement behavior as [String#getBytes()].
  /// Unlike [#encodedLength(String)], this method does not reject unpaired surrogate code units.
  public static int encodedLengthWithReplacement(String s) {
    int length = 0;
    int index = 0;
    while (index < s.length()) {
      char character = s.charAt(index);
      if (character <= 0x7f) {
        length++;
      } else if (character <= 0x7ff) {
        length += 2;
      } else if (Character.isHighSurrogate(character) && index + 1 < s.length()
          && Character.isLowSurrogate(s.charAt(index + 1))) {
        length += 4;
        index++;
      } else if (Character.isSurrogate(character)) {
        length++;
      } else {
        length += 3;
      }
      index++;
    }
    return length;
  }

  public static String decode(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }

  public static boolean isAscii(byte[] bytes) {
    int or = 0;
    for (byte b : bytes) {
      // Do not check within loop because most values are ASCII
      or |= b;
    }
    return (or & 0x80) == 0;
  }
}
