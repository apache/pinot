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
package org.apache.pinot.common.function;

import java.util.Map;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


public class CrossTableFunctionTest {

  @Test
  public void testMatchesEverySpelling() {
    // Underscores are dropped and the case is folded per character, so the dotless i (U+0131) that only such a fold
    // maps to i counts too; the parser's canonical name is one more spelling
    Map<String, CrossTableFunction> spellings = Map.ofEntries(
        Map.entry("LOOKUP", CrossTableFunction.LOOKUP),
        Map.entry("lookup", CrossTableFunction.LOOKUP),
        Map.entry("look_Up", CrossTableFunction.LOOKUP),
        Map.entry("IN_SUBQUERY", CrossTableFunction.IN_SUBQUERY),
        Map.entry("inSubquery", CrossTableFunction.IN_SUBQUERY),
        Map.entry("In_Sub_Query", CrossTableFunction.IN_SUBQUERY),
        Map.entry("IN_PARTITIONED_SUBQUERY", CrossTableFunction.IN_PARTITIONED_SUBQUERY),
        Map.entry("inPartitionedSubquery", CrossTableFunction.IN_PARTITIONED_SUBQUERY),
        Map.entry("In_Partitioned_Sub_Query", CrossTableFunction.IN_PARTITIONED_SUBQUERY),
        Map.entry("IN_PARTıTIONED_SUBQUERY", CrossTableFunction.IN_PARTITIONED_SUBQUERY),
        Map.entry(RequestUtils.canonicalizeFunctionName("IN_PARTITIONED_SUBQUERY"),
            CrossTableFunction.IN_PARTITIONED_SUBQUERY));
    for (Map.Entry<String, CrossTableFunction> entry : spellings.entrySet()) {
      assertSame(CrossTableFunction.fromFunctionName(entry.getKey()), entry.getValue(), entry.getKey());
      assertTrue(entry.getValue().matches(entry.getKey()), entry.getKey());
    }
  }

  @Test
  public void testOtherFunctionsDoNotMatch() {
    String[] others = {"inIdSet", "IN_ID_SET", "lookups", "subquery", "IN_PARTITIONED_SUBQUERY2", "partitioned", ""};
    for (String name : others) {
      assertNull(CrossTableFunction.fromFunctionName(name), name);
    }
    assertFalse(CrossTableFunction.IN_SUBQUERY.matches("inPartitionedSubquery"));
    assertFalse(CrossTableFunction.IN_PARTITIONED_SUBQUERY.matches("inSubquery"));
    // A parse tree node that is not a function call has no name
    assertFalse(CrossTableFunction.LOOKUP.matches(null));
    assertNull(CrossTableFunction.fromFunctionName(null));
  }
}
