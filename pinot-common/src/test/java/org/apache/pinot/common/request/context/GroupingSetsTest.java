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

import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class GroupingSetsTest {

  @Test
  public void testMaskAndParticipation() {
    int mask = GroupingSets.maskOf(0, 2);
    assertTrue(GroupingSets.participates(mask, 0));
    assertFalse(GroupingSets.participates(mask, 1));
    assertTrue(GroupingSets.participates(mask, 2));
    assertEquals(GroupingSets.maskOf(), 0);
  }

  @Test
  public void testGroupingIdValueMsbFirst() {
    // N=3 columns (a=0, b=1, c=2). $groupingId bit (N-1-i) set => column i rolled up.
    // All present => 0.
    assertEquals(GroupingSets.groupingIdValue(GroupingSets.maskOf(0, 1, 2), 3), 0b000);
    // Only a present (b,c rolled up) => bits for b (N-1-1=1) and c (N-1-2=0) => 0b011.
    assertEquals(GroupingSets.groupingIdValue(GroupingSets.maskOf(0), 3), 0b011);
    // Only a,b present (c rolled up) => bit for c (0) => 0b001.
    assertEquals(GroupingSets.groupingIdValue(GroupingSets.maskOf(0, 1), 3), 0b001);
    // Grand total (nothing present) => all bits => 0b111.
    assertEquals(GroupingSets.groupingIdValue(GroupingSets.maskOf(), 3), 0b111);
  }

  @Test
  public void testGroupingFunction() {
    int n = 3;
    // Set = {a} ($groupingId = 0b011: b,c rolled up).
    int gid = GroupingSets.groupingIdValue(GroupingSets.maskOf(0), n);
    assertEquals(GroupingSets.grouping(gid, n, 0), 0);   // a present
    assertEquals(GroupingSets.grouping(gid, n, 1), 1);   // b rolled up
    assertEquals(GroupingSets.grouping(gid, n, 2), 1);   // c rolled up
  }

  @Test
  public void testGroupingIdFunctionFirstArgMostSignificant() {
    int n = 3;
    // Set = {a} => GROUPING(a)=0, GROUPING(b)=1, GROUPING(c)=1.
    int gid = GroupingSets.groupingIdValue(GroupingSets.maskOf(0), n);
    // GROUPING_ID(a, b) = 2*GROUPING(a) + GROUPING(b) = 0*2 + 1 = 1.
    assertEquals(GroupingSets.groupingId(gid, n, new int[]{0, 1}), 0b01);
    // GROUPING_ID(b, c) = 2*1 + 1 = 3.
    assertEquals(GroupingSets.groupingId(gid, n, new int[]{1, 2}), 0b11);
    // GROUPING_ID(b, a) (order matters) = 2*GROUPING(b) + GROUPING(a) = 2*1 + 0 = 2.
    assertEquals(GroupingSets.groupingId(gid, n, new int[]{1, 0}), 0b10);
  }

  @Test
  public void testRollup() {
    // ROLLUP(a, b, c) over union indices [0,1,2] => (abc),(ab),(a),() finest first.
    List<Integer> masks = GroupingSets.rollup(new int[]{0, 1, 2});
    assertEquals(masks, Arrays.asList(
        GroupingSets.maskOf(0, 1, 2),
        GroupingSets.maskOf(0, 1),
        GroupingSets.maskOf(0),
        GroupingSets.maskOf()));
  }

  @Test
  public void testCube() {
    // CUBE(a, b) => power set: (ab),(a),(b),() — 2^2 = 4 sets, full set first, empty last.
    List<Integer> masks = GroupingSets.cube(new int[]{0, 1});
    assertEquals(masks.size(), 4);
    assertEquals(masks.get(0).intValue(), GroupingSets.maskOf(0, 1));
    assertEquals(masks.get(masks.size() - 1).intValue(), GroupingSets.maskOf());
    assertTrue(masks.contains(GroupingSets.maskOf(0)));
    assertTrue(masks.contains(GroupingSets.maskOf(1)));
  }

  @Test
  public void testCrossProductMixedForm() {
    // GROUP BY a, ROLLUP(b, c): elements [ [a], [bc, b, ()] ] => [abc, ab, a].
    List<Integer> elementA = List.of(GroupingSets.maskOf(0));
    List<Integer> elementRollupBc = GroupingSets.rollup(new int[]{1, 2});
    List<Integer> sets = GroupingSets.crossProduct(List.of(elementA, elementRollupBc));
    assertEquals(sets, Arrays.asList(
        GroupingSets.maskOf(0, 1, 2),
        GroupingSets.maskOf(0, 1),
        GroupingSets.maskOf(0)));
  }

  @Test
  public void testCrossProductSinglePlainGroupBy() {
    // Ordinary GROUP BY a, b => one element list each with a single mask => single full set.
    List<Integer> sets = GroupingSets.crossProduct(
        List.of(List.of(GroupingSets.maskOf(0)), List.of(GroupingSets.maskOf(1))));
    assertEquals(sets, List.of(GroupingSets.maskOf(0, 1)));
  }

  @Test
  public void testCrossProductPreservesDuplicates() {
    // GROUP BY a, ROLLUP(a): [ [a], [a, ()] ] => [a|a, a|()] = [a, a] — duplicates kept (SQL semantics).
    List<Integer> sets = GroupingSets.crossProduct(
        List.of(List.of(GroupingSets.maskOf(0)), GroupingSets.rollup(new int[]{0})));
    assertEquals(sets, Arrays.asList(GroupingSets.maskOf(0), GroupingSets.maskOf(0)));
  }

  @Test
  public void testExplicitGroupingSetsWithEmptySet() {
    // GROUPING SETS ((a,b),(a),()) => masks [ab, a, ()] as one element; cross-product with nothing else.
    List<Integer> element = Arrays.asList(
        GroupingSets.maskOf(0, 1), GroupingSets.maskOf(0), GroupingSets.maskOf());
    List<Integer> sets = GroupingSets.crossProduct(List.of(element));
    assertEquals(sets, element);
  }
}
