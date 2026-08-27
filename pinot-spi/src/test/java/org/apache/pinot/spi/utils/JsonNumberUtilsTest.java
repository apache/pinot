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

import org.testng.Assert;
import org.testng.annotations.Test;


/// Mirrors the JSON-long corpus in `NumberUtilsTest` so the shared parser stays aligned with
/// `NumberUtils.parseJsonLong`.
public class JsonNumberUtilsTest {

  @Test
  public void testParseJsonLong() {
    assertJsonLong("0", 0);
    assertJsonLong("-1", -1);
    assertJsonLong("-1000000", -1000000);
    assertJsonLong("-1223372036854775808", -1223372036854775808L);
    assertJsonLong("-9223372036854775808", Long.MIN_VALUE);
    assertJsonLong("9223372036854775807", Long.MAX_VALUE);
    assertJsonLong("1223372036854775807", 1223372036854775807L);

    // fp literals
    assertJsonLong("0.0", 0);
    assertJsonLong("-0.0", 0);
    assertJsonLong("1.", 1);
    assertJsonLong("+1.", 1);
    assertJsonLong("0.12345678", 0);
    assertJsonLong("-1000000.12345678", -1000000);
    assertJsonLong("1000000.12345678", 1000000);
    assertJsonLong("-9223372036854775808.123456", Long.MIN_VALUE);
    assertJsonLong("9223372036854775807.123456", Long.MAX_VALUE);
    assertJsonLong("1.9", 1);

    // with exponent
    assertJsonLong("2e0", 2L);
    assertJsonLong("2e1", 20L);
    assertJsonLong("2e2", 200L);
    assertJsonLong("1e10", 10000000000L);
    assertJsonLong("1e15", 1000000000000000L);
    assertJsonLong("1.1e10", 11000000000L);
    assertJsonLong("1.1E10", 11000000000L);
    assertJsonLong("1E1", 10L);

    assertJsonLongError(null);
    assertJsonLongError("");
    assertJsonLongError("q");
    assertJsonLongError("--1");
    assertJsonLongError("++1");
    assertJsonLongError("+1+");
    assertJsonLongError("+1-");
    assertJsonLongError("1e");
    assertJsonLongError("1E");
    assertJsonLongError("9223372036854775808");
    assertJsonLongError("19223372036854775808");
    assertJsonLongError("-9223372036854775809");
    assertJsonLongError("-19223372036854775808");

    // fp literals
    assertJsonLongError("1.Q");
    assertJsonLongError("1..");
    assertJsonLongError("1.1.");
    assertJsonLongError("1.1e");
    assertJsonLongError("1e");
    assertJsonLongError("1ee");

    // with exponent
    assertJsonLongError("2E+");
    assertJsonLongError("2E-");
    assertJsonLongError("2E20");
    assertJsonLongError("2E100");
    assertJsonLongError("2E100.123");
    assertJsonLongError("2E-1");
  }

  private void assertJsonLong(String input, long expected) {
    Assert.assertEquals(JsonNumberUtils.parseJsonLong(input), expected);
  }

  private void assertJsonLongError(String input) {
    Assert.assertThrows(NumberFormatException.class, () -> JsonNumberUtils.parseJsonLong(input));
  }
}
