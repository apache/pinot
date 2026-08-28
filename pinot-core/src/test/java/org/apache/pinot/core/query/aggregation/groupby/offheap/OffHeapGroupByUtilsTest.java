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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class OffHeapGroupByUtilsTest {

  @Test
  public void testEncodeUtf8MatchesJdkEncoder() {
    // Deliberate boundary and malformed cases. The encoder must be byte-for-byte identical to
    // String#getBytes(StandardCharsets.UTF_8), including '?' replacement of unpaired surrogates.
    String[] cases = {
        "",
        "ascii only",
        " ",
        "",                 // 1-byte upper bound
        "",                 // 2-byte lower bound
        "߿",                 // 2-byte upper bound
        "ࠀ",                 // 3-byte lower bound
        "퟿",                 // last char before the surrogate range
        "",                 // first char after the surrogate range
        "￿",                 // 3-byte upper bound
        "café 你好", // mixed 1/2/3-byte
        "😀",           // valid surrogate pair (emoji, 4-byte)
        "a😀b🎉c", // pairs embedded in text
        "\uD800",                 // unpaired high surrogate at end
        "\uDC00",                 // unpaired low surrogate
        "\uD800a",                // high surrogate followed by a normal char
        "\uD800𐀀",     // unpaired high followed by a valid pair
        "\uDC00\uD800",           // low then high (both unpaired)
        "x\uD800",                // trailing unpaired high
    };
    for (String value : cases) {
      assertEncodeMatches(value);
    }

    // Randomized: arbitrary char sequences (freely mixing valid text and surrogate salad)
    Random random = new Random(42);
    for (int i = 0; i < 10_000; i++) {
      int length = random.nextInt(32);
      char[] chars = new char[length];
      int charIndex = 0;
      while (charIndex < length) {
        switch (random.nextInt(5)) {
          case 0:
            chars[charIndex++] = (char) random.nextInt(0x80);
            break;
          case 1:
            chars[charIndex++] = (char) random.nextInt(0x800);
            break;
          case 2:
            chars[charIndex++] = (char) random.nextInt(0x10000);
            break;
          case 3:
            chars[charIndex++] = (char) (Character.MIN_SURROGATE + random.nextInt(
                Character.MAX_SURROGATE - Character.MIN_SURROGATE + 1));
            break;
          default:
            // Frequently emit valid pairs so the 4-byte path is well covered
            if (charIndex + 1 < length) {
              int codePoint = 0x10000 + random.nextInt(0x100000);
              chars[charIndex++] = Character.highSurrogate(codePoint);
              chars[charIndex++] = Character.lowSurrogate(codePoint);
            } else {
              chars[charIndex++] = 'z';
            }
            break;
        }
      }
      assertEncodeMatches(new String(chars));
    }
  }

  private static void assertEncodeMatches(String value) {
    byte[] scratch = new byte[value.length() * 3 + 1];
    int length = OffHeapGroupByUtils.encodeUtf8(value, scratch);
    byte[] expected = value.getBytes(StandardCharsets.UTF_8);
    assertEquals(Arrays.copyOf(scratch, length), expected,
        "encodeUtf8 mismatch for chars: " + Arrays.toString(value.chars().toArray()));
  }

  @Test
  public void testPackUnpackIntsRoundTrip() {
    Random random = new Random(42);
    for (int iteration = 0; iteration < 1000; iteration++) {
      int numValues = 1 + random.nextInt(8);
      int[] values = new int[numValues];
      for (int i = 0; i < numValues; i++) {
        // Include negative sentinels like ID_FOR_NULL (-2) and extremes
        switch (random.nextInt(4)) {
          case 0:
            values[i] = random.nextInt();
            break;
          case 1:
            values[i] = -2;
            break;
          case 2:
            values[i] = Integer.MIN_VALUE;
            break;
          default:
            values[i] = random.nextInt(100);
            break;
        }
      }
      byte[] scratch = new byte[numValues * Integer.BYTES];
      int length = OffHeapGroupByUtils.packInts(values, numValues, scratch);
      assertEquals(length, numValues * Integer.BYTES);
      int[] unpacked = new int[numValues];
      OffHeapGroupByUtils.unpackInts(scratch, numValues, unpacked);
      assertEquals(unpacked, values);
    }
  }

  @Test
  public void testEnsureByteCapacity() {
    byte[] scratch = new byte[8];
    assertEquals(OffHeapGroupByUtils.ensureByteCapacity(scratch, 8), scratch);
    assertEquals(OffHeapGroupByUtils.ensureByteCapacity(scratch, 4), scratch);
    assertEquals(OffHeapGroupByUtils.ensureByteCapacity(scratch, 9).length, 16);
    assertEquals(OffHeapGroupByUtils.ensureByteCapacity(scratch, 100).length, 100);
  }
}
