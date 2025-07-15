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

import static org.apache.pinot.common.function.scalar.regexp.RegexpLikeVarFunctions.likeVar;
import static org.apache.pinot.common.function.scalar.regexp.RegexpLikeVarFunctions.regexpLikeVar;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RegexpLikeVarFunctionsTest {

  @Test
  public void testLike() {
    assertTrue(likeVar("ab", "%ab%"));
    assertTrue(likeVar("aaba", "%ab%"));
    assertTrue(likeVar("$ab$", "%ab%"));

    assertFalse(likeVar("", "%ab%"));
    assertFalse(likeVar("_", "%ab%"));
    assertFalse(likeVar("a", "%ab%"));
    assertFalse(likeVar("b", "%ab%"));

    assertFalse(likeVar("aab", "ab"));
  }

  @Test
  public void testRegexpLike() {
    assertTrue(regexpLikeVar("ab", ".*ab.*"));
    assertTrue(regexpLikeVar("aaba", ".*ab.*"));
    assertTrue(regexpLikeVar("$ab$", ".*ab.*"));

    assertFalse(regexpLikeVar("", ".*ab.*"));
    assertFalse(regexpLikeVar("_", ".*ab.*"));
    assertFalse(regexpLikeVar("a", ".*ab.*"));
    assertFalse(regexpLikeVar("b", ".*ab.*"));

    //returns true because function matches against first pattern
    assertFalse(regexpLikeVar("aab", "abb"));
  }

  @Test
  public void testRegexpLikeWithMatchParameters() {
    // Test case-sensitive (default)
    assertFalse(regexpLikeVar("Hello", "hello", "c")); // Different case, should not match
    assertTrue(regexpLikeVar("hello", "hello", "c"));  // Same case, should match
    assertTrue(regexpLikeVar("HELLO", "HELLO", "c"));  // Same case, should match

    // Test case-insensitive
    assertTrue(regexpLikeVar("Hello", "hello", "i"));  // Different case, should match
    assertTrue(regexpLikeVar("HELLO", "hello", "i"));  // Different case, should match
    assertTrue(regexpLikeVar("hello", "HELLO", "i"));  // Different case, should match
    assertTrue(regexpLikeVar("hElLo", "HeLlO", "i"));  // Different case, should match
    assertFalse(regexpLikeVar("world", "hello", "i")); // Different word, should not match

    // Test with regex patterns
    assertTrue(regexpLikeVar("Hello World", "hello.*", "i"));  // Case-insensitive regex
    assertTrue(regexpLikeVar("HELLO WORLD", "hello.*", "i"));  // Case-insensitive regex
    assertFalse(regexpLikeVar("Hello World", "hello.*", "c")); // Case-sensitive regex
    assertTrue(regexpLikeVar("hello world", "hello.*", "c"));  // Case-sensitive regex

    // Test with special characters
    assertTrue(regexpLikeVar("Test123", "test\\d+", "i"));  // Case-insensitive with digits
    assertFalse(regexpLikeVar("Test123", "test\\d+", "c")); // Case-sensitive with digits
    assertTrue(regexpLikeVar("test123", "test\\d+", "c"));  // Case-sensitive with digits
  }

  @Test
  public void testRegexpLikeWithInvalidMatchParameters() {
    // Test invalid match parameters
    try {
      regexpLikeVar("test", "test", "x");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unsupported match parameter"));
    }

    try {
      regexpLikeVar("test", "test", "ix");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Unsupported match parameter"));
    }

    try {
      regexpLikeVar("test", "test", "ci");
      assertFalse(true, "Should throw exception for invalid match parameter");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid match parameter"));
    }
  }

  @Test
  public void testRegexpLikeCaseInsensitiveOptimization() {
    // Test that case-insensitive matching works correctly
    assertTrue(regexpLikeVar("Hello World", "hello", "i"));
    assertTrue(regexpLikeVar("HELLO WORLD", "hello", "i"));
    assertTrue(regexpLikeVar("hElLo WoRlD", "hello", "i"));

    // Test with different patterns to ensure each call compiles fresh
    assertTrue(regexpLikeVar("Test String", "test", "i"));
    assertTrue(regexpLikeVar("TEST STRING", "test", "i"));
  }
}
