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
package org.apache.pinot.common.request.context;

import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Pins the GROUPING()/GROUPING_ID() value contract shared by the single-stage reduce extractors and the
/// multi-stage planner projection: bit = 1 iff the argument column is rolled up (absent from the set), packed
/// with the first argument as the most significant bit, indexed by the grouping set's ordinal.
public class GroupingSetsTest {

  @Test
  public void testGroupingValuePacksRolledUpBitsMsbFirst() {
    /// ROLLUP(c0, c1) sets: (c0, c1), (c0), () — args (c0, c1) pack as GROUPING(c0) << 1 | GROUPING(c1).
    List<int[]> rollupSets = List.of(new int[]{0, 1}, new int[]{0}, new int[]{});
    assertEquals(GroupingSets.groupingValuesByOrdinal(rollupSets, new int[]{0, 1}), new int[]{0b00, 0b01, 0b11});
    /// Argument order defines bit order: reversed args produce the mirrored packing.
    assertEquals(GroupingSets.groupingValuesByOrdinal(rollupSets, new int[]{1, 0}), new int[]{0b00, 0b10, 0b11});
    /// Single-argument GROUPING(c1): 0 where c1 participates, 1 where rolled up.
    assertEquals(GroupingSets.groupingValuesByOrdinal(rollupSets, new int[]{1}), new int[]{0, 1, 1});
    /// The boxed (wire / plan representation) overload computes identical values.
    assertEquals(
        GroupingSets.groupingValuesByOrdinal(List.of(List.of(0, 1), List.of(0), List.of()), List.of(0, 1)),
        new int[]{0b00, 0b01, 0b11});
  }

  @Test
  public void testGroupingValueUnboundedColumns() {
    /// Grouping columns are unlimited; only the per-call argument count is capped. A set over high column
    /// indexes (way past the retired 31-column bitmask range) computes fine.
    List<int[]> sets = List.of(new int[]{0, 40}, new int[]{40}, new int[]{});
    assertEquals(GroupingSets.groupingValuesByOrdinal(sets, new int[]{40}), new int[]{0, 0, 1});
    assertEquals(GroupingSets.groupingValuesByOrdinal(sets, new int[]{0, 40}), new int[]{0b00, 0b10, 0b11});
  }

  @Test
  public void testGroupingValueRejectsTooManyArguments() {
    /// The packed value is an INT, so a single GROUPING/GROUPING_ID call accepts at most 31 arguments.
    List<int[]> sets = List.of(new int[]{0});
    assertEquals(GroupingSets.groupingValuesByOrdinal(sets, new int[GroupingSets.MAX_GROUPING_FUNCTION_ARGS]).length,
        1);
    assertThrows(IllegalStateException.class,
        () -> GroupingSets.groupingValuesByOrdinal(sets, new int[GroupingSets.MAX_GROUPING_FUNCTION_ARGS + 1]));
  }
}
