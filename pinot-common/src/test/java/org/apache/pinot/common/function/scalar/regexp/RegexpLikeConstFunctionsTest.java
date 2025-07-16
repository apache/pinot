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
package org.apache.pinot.common.function.scalar.regexp;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RegexpLikeConstFunctionsTest {

  @Test
  public void testLike() {
    RegexpLikeConstFunctions f = new RegexpLikeConstFunctions();

    assertTrue(f.like("ab", "%ab%"));
    assertTrue(f.like("aaba", "%ab%"));
    assertTrue(f.like("$ab$", "%ab%"));

    assertFalse(f.like("", "%ab%"));
    assertFalse(f.like("_", "%ab%"));
    assertFalse(f.like("a", "%ab%"));
    assertFalse(f.like("b", "%ab%"));

    //returns true because function matches against first pattern
    assertTrue(f.like("aab", "abb"));
  }

  @Test
  public void testRegexpLike() {
    RegexpLikeConstFunctions f = new RegexpLikeConstFunctions();

    assertTrue(f.regexpLike("ab", ".*ab.*"));
    assertTrue(f.regexpLike("aaba", ".*ab.*"));
    assertTrue(f.regexpLike("$ab$", ".*ab.*"));

    assertFalse(f.regexpLike("", ".*ab.*"));
    assertFalse(f.regexpLike("_", ".*ab.*"));
    assertFalse(f.regexpLike("a", ".*ab.*"));
    assertFalse(f.regexpLike("b", ".*ab.*"));

    //returns true because function matches against first pattern
    assertTrue(f.regexpLike("aab", "ab"));
  }

  @Test
  public void testRegexpLikeWithMatchParameters() {
    // Test case-sensitive (default)
    RegexpLikeConstFunctions f1 = new RegexpLikeConstFunctions();
    assertFalse(f1.regexpLike("Hello", "hello", "c"));
    assertTrue(f1.regexpLike("hello", "hello", "c"));

    // Test case-insensitive
    RegexpLikeConstFunctions f2 = new RegexpLikeConstFunctions();
    assertTrue(f2.regexpLike("Hello", "hello", "i"));
    assertTrue(f2.regexpLike("HELLO", "hello", "i"));
    assertTrue(f2.regexpLike("hello", "HELLO", "i"));
    assertTrue(f2.regexpLike("hElLo", "HeLlO", "i"));
    assertFalse(f2.regexpLike("world", "hello", "i"));

    // Test with regex patterns - case insensitive
    RegexpLikeConstFunctions f3 = new RegexpLikeConstFunctions();
    assertTrue(f3.regexpLike("Hello World", "hello.*", "i"));
    assertTrue(f3.regexpLike("HELLO WORLD", "hello.*", "i"));

    // Test with regex patterns - case sensitive
    RegexpLikeConstFunctions f4 = new RegexpLikeConstFunctions();
    assertFalse(f4.regexpLike("Hello World", "hello.*", "c"));
    assertTrue(f4.regexpLike("hello world", "hello.*", "c"));

    // Test with special characters - case insensitive
    RegexpLikeConstFunctions f5 = new RegexpLikeConstFunctions();
    assertTrue(f5.regexpLike("Test123", "test\\d+", "i"));

    // Test with special characters - case sensitive
    RegexpLikeConstFunctions f6 = new RegexpLikeConstFunctions();
    assertFalse(f6.regexpLike("Test123", "test\\d+", "c"));
    assertTrue(f6.regexpLike("test123", "test\\d+", "c"));
  }

  @Test
  public void testRegexpLikeWithInvalidMatchParameters() {
    RegexpLikeConstFunctions f = new RegexpLikeConstFunctions();

    // Test invalid match parameters
    try {
      f.regexpLike("test", "test", "x");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unsupported match parameter"));
    }

    try {
      f.regexpLike("test", "test", "ix");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Match parameter must be exactly one character"));
    }

    try {
      f.regexpLike("test", "test", "ci");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Match parameter must be exactly one character"));
    }
  }

  @Test
  public void testRegexpLikeCaseInsensitiveOptimization() {
    // Test that case-insensitive matching works correctly
    RegexpLikeConstFunctions f1 = new RegexpLikeConstFunctions();
    assertTrue(f1.regexpLike("Hello World", "hello", "i"));

    RegexpLikeConstFunctions f2 = new RegexpLikeConstFunctions();
    assertTrue(f2.regexpLike("HELLO WORLD", "hello", "i"));

    RegexpLikeConstFunctions f3 = new RegexpLikeConstFunctions();
    assertTrue(f3.regexpLike("hElLo WoRlD", "hello", "i"));
  }

  @Test
  public void testRegexpLikeWithUppercaseMatchParameters() {
    // Test uppercase 'I' for case-insensitive
    RegexpLikeConstFunctions f1 = new RegexpLikeConstFunctions();
    assertTrue(f1.regexpLike("Hello", "hello", "I"));

    RegexpLikeConstFunctions f2 = new RegexpLikeConstFunctions();
    assertTrue(f2.regexpLike("HELLO", "hello", "I"));

    RegexpLikeConstFunctions f3 = new RegexpLikeConstFunctions();
    assertTrue(f3.regexpLike("hello", "HELLO", "I"));

    // Test uppercase 'C' for case-sensitive
    RegexpLikeConstFunctions f4 = new RegexpLikeConstFunctions();
    assertFalse(f4.regexpLike("Hello", "hello", "C"));

    RegexpLikeConstFunctions f5 = new RegexpLikeConstFunctions();
    assertTrue(f5.regexpLike("hello", "hello", "C"));

    RegexpLikeConstFunctions f6 = new RegexpLikeConstFunctions();
    assertTrue(f6.regexpLike("HELLO", "HELLO", "C"));
  }
}
