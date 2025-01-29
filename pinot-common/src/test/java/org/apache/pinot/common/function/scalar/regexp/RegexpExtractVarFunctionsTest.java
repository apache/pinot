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

import static org.apache.pinot.common.function.scalar.regexp.RegexpExtractVarFunctions.regexpExtract;
import static org.testng.Assert.assertEquals;


public class RegexpExtractVarFunctionsTest {

  @Test
  public void test() {
    assertEquals(regexpExtract("val abe eee", "(a[bcd]e)"), "abe");
    assertEquals(regexpExtract("val ade eee", "(a[bcd]e)"), "ade");
    assertEquals(regexpExtract("val age eee", "(a[bcd]e)"), "");

    assertEquals(regexpExtract("val abe ace", "(a[bcd]e) (a[bcd]e)", 2), "ace");
    assertEquals(regexpExtract("abe ace ade", "(a[bcd]e) (a[bcd]e) (a[bcd]e)", 3), "ade");

    assertEquals(regexpExtract("abe ace ade", "(a[bcd]e)", 5, "wrong"), "wrong");
    assertEquals(regexpExtract("aa bb cc", "(a[bcd]e)", 1, "wrong"), "wrong");
  }
}
