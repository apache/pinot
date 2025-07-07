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


public class RegexpLikeCiConstFunctionsTest {

  @Test
  public void testRegexpLikeCi() {
    RegexpLikeCiConstFunctions f = new RegexpLikeCiConstFunctions();

    // Case-insensitive matching
    assertTrue(f.regexpLikeCi("Hello", "hello"));
    assertTrue(f.regexpLikeCi("HELLO", "hello"));
    assertTrue(f.regexpLikeCi("hello", "HELLO"));
    assertTrue(f.regexpLikeCi("Hello World", ".*world.*"));
    assertTrue(f.regexpLikeCi("HELLO WORLD", ".*world.*"));
    assertTrue(f.regexpLikeCi("hello world", ".*WORLD.*"));

    // Case-sensitive patterns should still work
    assertTrue(f.regexpLikeCi("ab", ".*ab.*"));
    assertTrue(f.regexpLikeCi("aaba", ".*ab.*"));
    assertTrue(f.regexpLikeCi("$ab$", ".*ab.*"));

    // Negative cases
    assertFalse(f.regexpLikeCi("", ".*ab.*"));
    assertFalse(f.regexpLikeCi("_", ".*ab.*"));
    assertFalse(f.regexpLikeCi("a", ".*ab.*"));
    assertFalse(f.regexpLikeCi("b", ".*ab.*"));
    assertFalse(f.regexpLikeCi("xyz", "hello"));
    assertFalse(f.regexpLikeCi("HELLO", "world"));
  }
}
