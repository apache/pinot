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

    assertTrue(f.likeConst("ab", "%ab%"));
    assertTrue(f.likeConst("aaba", "%ab%"));
    assertTrue(f.likeConst("$ab$", "%ab%"));

    assertFalse(f.likeConst("", "%ab%"));
    assertFalse(f.likeConst("_", "%ab%"));
    assertFalse(f.likeConst("a", "%ab%"));
    assertFalse(f.likeConst("b", "%ab%"));

    //returns true because function matches against first pattern
    assertTrue(f.likeConst("aab", "abb"));
  }

  @Test
  public void testRegexpLike() {
    RegexpLikeConstFunctions f = new RegexpLikeConstFunctions();

    assertTrue(f.regexpLikeConst("ab", ".*ab.*"));
    assertTrue(f.regexpLikeConst("aaba", ".*ab.*"));
    assertTrue(f.regexpLikeConst("$ab$", ".*ab.*"));

    assertFalse(f.regexpLikeConst("", ".*ab.*"));
    assertFalse(f.regexpLikeConst("_", ".*ab.*"));
    assertFalse(f.regexpLikeConst("a", ".*ab.*"));
    assertFalse(f.regexpLikeConst("b", ".*ab.*"));

    //returns true because function matches against first pattern
    assertTrue(f.regexpLikeConst("aab", "ab"));
  }
}
