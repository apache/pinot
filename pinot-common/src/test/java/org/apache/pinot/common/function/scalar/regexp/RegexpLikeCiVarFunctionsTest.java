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


public class RegexpLikeCiVarFunctionsTest {

  @Test
  public void testRegexpLikeCi() {
    // Case-insensitive matching
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("Hello", "hello"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("HELLO", "hello"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("hello", "HELLO"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("Hello World", ".*world.*"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("HELLO WORLD", ".*world.*"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("hello world", ".*WORLD.*"));

    // Case-sensitive patterns should still work
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("ab", ".*ab.*"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("aaba", ".*ab.*"));
    assertTrue(RegexpLikeVarFunctions.regexpLikeCiVar("$ab$", ".*ab.*"));

    // Negative cases
    assertFalse(RegexpLikeVarFunctions.regexpLikeCiVar("", ".*ab.*"));
    assertFalse(RegexpLikeVarFunctions.regexpLikeCiVar("_", ".*ab.*"));
    assertFalse(RegexpLikeVarFunctions.regexpLikeCiVar("a", ".*ab.*"));
    assertFalse(RegexpLikeVarFunctions.regexpLikeCiVar("b", ".*ab.*"));
    assertFalse(RegexpLikeVarFunctions.regexpLikeCiVar("xyz", "hello"));
    assertFalse(RegexpLikeVarFunctions.regexpLikeCiVar("HELLO", "world"));
  }
}
