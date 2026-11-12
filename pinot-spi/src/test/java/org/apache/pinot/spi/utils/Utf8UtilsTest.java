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

import java.util.Arrays;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/// Tests [Utf8Utils].
public class Utf8UtilsTest {
  @DataProvider
  public Object[][] encodedLengths() {
    return new Object[][]{
        {"", 0},
        {"ascii", 5},
        // Two-byte characters
        {"caf\u00e9", 5},
        {"\u00e9\u00e9", 4},
        // Three-byte characters
        {"\u4e2d\u6587", 6},
        {"a\u20acb", 5},
        // Surrogate pairs encode to four bytes
        {"\uD83D\uDE00", 4},
        {"a\uD83D\uDE00b", 6},
        // Unpaired surrogates encode to a single replacement byte
        {"\uD800", 1},
        {"abc\uD800", 4},
        {"a\uD800b", 3},
        {"\uDC00", 1},
        {"\uDC00\uD800", 2},
        {"\uD800\uD83D\uDE00", 5}
    };
  }

  @Test(dataProvider = "encodedLengths")
  public void testEncodedLength(String s, int expectedLength) {
    assertEquals(Utf8Utils.encodedLength(s), expectedLength);
    assertEquals(Utf8Utils.encode(s).length, expectedLength);
  }

  @Test
  public void testIsAscii() {
    // Cover the empty array and every tail length around the 8-byte stride
    for (int length = 0; length <= 24; length++) {
      byte[] bytes = new byte[length];
      Arrays.fill(bytes, (byte) 'a');
      assertTrue(Utf8Utils.isAscii(bytes), "length: " + length);
      for (int i = 0; i < length; i++) {
        bytes[i] = 0x7f;
        assertTrue(Utf8Utils.isAscii(bytes), "length: " + length + ", index: " + i);
        bytes[i] = (byte) 0x80;
        assertFalse(Utf8Utils.isAscii(bytes), "length: " + length + ", index: " + i);
        bytes[i] = (byte) 0xff;
        assertFalse(Utf8Utils.isAscii(bytes), "length: " + length + ", index: " + i);
        bytes[i] = 'a';
      }
    }
    assertTrue(Utf8Utils.isAscii(Utf8Utils.encode("ascii")));
    assertFalse(Utf8Utils.isAscii(Utf8Utils.encode("caf\u00e9")));
  }
}
