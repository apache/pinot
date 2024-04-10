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

import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class Base64UtilsTest {
  private static final String SPECIAL_CHARS = "+/=-_";
  private static final String UPPER_CASE_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final String LOWER_CASE_CHARS = "abcdefghijklmnopqrstuvwxyz";
  private static final String NUMBER_CHARS = "0123456789";
  private static final String[] BASE64_STRINGS = {
      UPPER_CASE_CHARS,
      LOWER_CASE_CHARS,
      SPECIAL_CHARS,
      NUMBER_CHARS,
      SPECIAL_CHARS + NUMBER_CHARS + LOWER_CASE_CHARS + UPPER_CASE_CHARS,
      UPPER_CASE_CHARS + SPECIAL_CHARS + LOWER_CASE_CHARS + NUMBER_CHARS
  };
  private static final String[] BASE64_STRINGS_WITH_WHITE_SPACE = {
      SPECIAL_CHARS + "\n" + NUMBER_CHARS + "\t" + LOWER_CASE_CHARS + " " + UPPER_CASE_CHARS,
      UPPER_CASE_CHARS + "\n" + SPECIAL_CHARS + "\t" + LOWER_CASE_CHARS + " " + NUMBER_CHARS
  };
  private static final String[] NON_BASE64_STRINGS = {
      UPPER_CASE_CHARS + "!",
      LOWER_CASE_CHARS + "@",
      SPECIAL_CHARS + "#",
      NUMBER_CHARS + "$",
      SPECIAL_CHARS + ".." + NUMBER_CHARS + "?" + LOWER_CASE_CHARS + "^" + UPPER_CASE_CHARS + "*"
  };

  @Test
  public void testBase64IgnoreWhiteSpace() {
    for (final String s : BASE64_STRINGS) {
      assertTrue(Base64Utils.isBase64IgnoreWhiteSpace(s.getBytes(StandardCharsets.UTF_8)));
      assertFalse(Base64Utils.isBase64IgnoreWhiteSpace((s + "..").getBytes(StandardCharsets.UTF_8)));
    }

    for (final String s : BASE64_STRINGS_WITH_WHITE_SPACE) {
      assertTrue(Base64Utils.isBase64IgnoreWhiteSpace(s.getBytes(StandardCharsets.UTF_8)));
      assertFalse(Base64Utils.isBase64IgnoreWhiteSpace((s + "..").getBytes(StandardCharsets.UTF_8)));
    }

    for (final String s : NON_BASE64_STRINGS) {
      assertFalse(Base64Utils.isBase64IgnoreWhiteSpace(s.getBytes(StandardCharsets.UTF_8)));
      assertFalse(Base64Utils.isBase64IgnoreWhiteSpace((s + "..").getBytes(StandardCharsets.UTF_8)));
    }
  }

  @Test
  public void testBase64IgnoreTrailingPeriods() {
    for (final String s : BASE64_STRINGS) {
      String testStr = s;
      for (int i = 0; i < 10; i++) {
        assertTrue(Base64Utils.isBase64IgnoreTrailingPeriods(testStr.getBytes(StandardCharsets.UTF_8)));
        testStr = testStr + ".";
      }
    }

    for (final String s : BASE64_STRINGS_WITH_WHITE_SPACE) {
      String testStr = s;
      for (int i = 0; i < 2; i++) {
        assertFalse(Base64Utils.isBase64IgnoreTrailingPeriods(testStr.getBytes(StandardCharsets.UTF_8)));
        testStr = testStr + ".";
      }
    }

    for (final String s : NON_BASE64_STRINGS) {
      String testStr = s;
      for (int i = 0; i < 2; i++) {
        assertFalse(Base64Utils.isBase64IgnoreTrailingPeriods(testStr.getBytes(StandardCharsets.UTF_8)));
        testStr = testStr + ".";
      }
    }
  }
}
