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

import static org.apache.pinot.common.function.scalar.regexp.RegexpLikeVarFunctions.like;
import static org.apache.pinot.common.function.scalar.regexp.RegexpLikeVarFunctions.regexpLike;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RegexpLikeVarFunctionsTest {

  @Test
  public void testLike() {
    assertTrue(like("ab", "%ab%"));
    assertTrue(like("aaba", "%ab%"));
    assertTrue(like("$ab$", "%ab%"));

    assertFalse(like("", "%ab%"));
    assertFalse(like("_", "%ab%"));
    assertFalse(like("a", "%ab%"));
    assertFalse(like("b", "%ab%"));

    assertFalse(like("aab", "ab"));
  }

  @Test
  public void testRegexpLike() {
    assertTrue(regexpLike("ab", ".*ab.*"));
    assertTrue(regexpLike("aaba", ".*ab.*"));
    assertTrue(regexpLike("$ab$", ".*ab.*"));

    assertFalse(regexpLike("", ".*ab.*"));
    assertFalse(regexpLike("_", ".*ab.*"));
    assertFalse(regexpLike("a", ".*ab.*"));
    assertFalse(regexpLike("b", ".*ab.*"));

    //returns true because function matches against first pattern
    assertFalse(regexpLike("aab", "abb"));
  }
}
