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
    RegexpLikeConstFunctions f = new RegexpLikeConstFunctions();

    // Test case-sensitive (default)
    assertFalse(f.regexpLike("Hello", "hello", "c")); // Different case, should not match
    assertTrue(f.regexpLike("hello", "hello", "c"));  // Same case, should match
    assertTrue(f.regexpLike("HELLO", "HELLO", "c"));  // Same case, should match

    // Test case-insensitive
    assertTrue(f.regexpLike("Hello", "hello", "i"));  // Different case, should match
    assertTrue(f.regexpLike("HELLO", "hello", "i"));  // Different case, should match
    assertTrue(f.regexpLike("hello", "HELLO", "i"));  // Different case, should match
    assertTrue(f.regexpLike("hElLo", "HeLlO", "i"));  // Different case, should match
    assertFalse(f.regexpLike("world", "hello", "i")); // Different word, should not match

    // Test with regex patterns
    assertTrue(f.regexpLike("Hello World", "hello.*", "i"));  // Case-insensitive regex
    assertTrue(f.regexpLike("HELLO WORLD", "hello.*", "i"));  // Case-insensitive regex
    assertFalse(f.regexpLike("Hello World", "hello.*", "c")); // Case-sensitive regex
    assertTrue(f.regexpLike("hello world", "hello.*", "c"));  // Case-sensitive regex

    // Test with special characters
    assertTrue(f.regexpLike("Test123", "test\\d+", "i"));  // Case-insensitive with digits
    assertFalse(f.regexpLike("Test123", "test\\d+", "c")); // Case-sensitive with digits
    assertTrue(f.regexpLike("test123", "test\\d+", "c"));  // Case-sensitive with digits
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
      assertTrue(e.getMessage().contains("Unsupported match parameter"));
    }

    try {
      f.regexpLike("test", "test", "ci");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid match parameter"));
    }
  }

  @Test
  public void testRegexpLikeCaseInsensitiveOptimization() {
    RegexpLikeConstFunctions f = new RegexpLikeConstFunctions();

    // Test that case-insensitive matching works correctly
    assertTrue(f.regexpLike("Hello World", "hello", "i"));
    assertTrue(f.regexpLike("HELLO WORLD", "hello", "i"));
    assertTrue(f.regexpLike("hElLo WoRlD", "hello", "i"));

    // Test with different patterns to ensure matcher is reused correctly
    assertTrue(f.regexpLike("Test String", "test", "i"));
    assertTrue(f.regexpLike("TEST STRING", "test", "i"));
  }
}
