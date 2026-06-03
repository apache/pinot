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
package org.apache.pinot.common.function.scalar;

import org.apache.pinot.common.request.context.GroupingSets;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class GroupingSetFunctionsTest {

  @Test
  public void testGrouping() {
    // N=3, set {a}: $groupingId = 0b011 (b and c rolled up). GROUPING(a) reads shift N-1-0=2, GROUPING(b) shift 1, etc.
    int groupingId = GroupingSets.groupingIdValue(GroupingSets.maskOf(0), 3);
    assertEquals(GroupingSetFunctions.grouping(groupingId, 2), 0);   // a present
    assertEquals(GroupingSetFunctions.grouping(groupingId, 1), 1);   // b rolled up
    assertEquals(GroupingSetFunctions.grouping(groupingId, 0), 1);   // c rolled up
  }

  @Test
  public void testGroupingId() {
    int groupingId = GroupingSets.groupingIdValue(GroupingSets.maskOf(0), 3);
    // GROUPING_ID(a, b) = 2*GROUPING(a) + GROUPING(b) = 0*2 + 1 = 1, gathered at shifts [2, 1].
    assertEquals(GroupingSetFunctions.groupingId(groupingId, new int[]{2, 1}), 0b01);
    // GROUPING_ID(b, c) = 2*1 + 1 = 3, gathered at shifts [1, 0].
    assertEquals(GroupingSetFunctions.groupingId(groupingId, new int[]{1, 0}), 0b11);
  }

  @Test
  public void testGroupingIdConsistentWithUtil() {
    // The scalar (shift form) must agree with the GroupingSets util (index form) for every column.
    int n = 4;
    int groupingId = GroupingSets.groupingIdValue(GroupingSets.maskOf(1, 3), n);
    for (int col = 0; col < n; col++) {
      assertEquals(GroupingSetFunctions.grouping(groupingId, n - 1 - col), GroupingSets.grouping(groupingId, n, col));
    }
  }
}
