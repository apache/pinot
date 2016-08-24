/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.operator.filter;

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.core.operator.filter.IntRanges;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Test for IntRanges.
 */
public class IntRangesTest {
  @Test
  public void testClip() {
    Pairs.IntPair range = new Pairs.IntPair(10, 20);
    IntRanges.clip(range, 0, 100);

    assertEquals(range.getLeft(), 10);
    assertEquals(range.getRight(), 20);

    IntRanges.clip(range, 0, 18);
    assertEquals(range.getLeft(), 10);
    assertEquals(range.getRight(), 18);

    IntRanges.clip(range, 12, 30);
    assertEquals(range.getLeft(), 12);
    assertEquals(range.getRight(), 18);

    IntRanges.clip(range, 14, 16);
    assertEquals(range.getLeft(), 14);
    assertEquals(range.getRight(), 16);
  }

  @Test
  public void testIsDegenerate() {
    Pairs.IntPair valid = new Pairs.IntPair(10, 20);
    assertFalse(IntRanges.isInvalid(valid));

    valid = new Pairs.IntPair(15, 15);
    assertFalse(IntRanges.isInvalid(valid));

    Pairs.IntPair invalid = new Pairs.IntPair(15, 14);
    assertTrue(IntRanges.isInvalid(invalid));
  }

  @Test
  public void testRangesAreMergeable() {
    Pairs.IntPair disjointA = new Pairs.IntPair(0, 10);
    Pairs.IntPair disjointB = new Pairs.IntPair(12, 20);
    assertFalse(IntRanges.rangesAreMergeable(disjointA, disjointB));
    assertFalse(IntRanges.rangesAreMergeable(disjointB, disjointA));

    Pairs.IntPair adjacentA = new Pairs.IntPair(0, 10);
    Pairs.IntPair adjacentB = new Pairs.IntPair(11, 20);
    assertTrue(IntRanges.rangesAreMergeable(adjacentA, adjacentB));
    assertTrue(IntRanges.rangesAreMergeable(adjacentB, adjacentA));

    Pairs.IntPair overlappingA = new Pairs.IntPair(0, 10);
    Pairs.IntPair overlappingB = new Pairs.IntPair(10, 15);
    assertTrue(IntRanges.rangesAreMergeable(overlappingA, overlappingB));
    assertTrue(IntRanges.rangesAreMergeable(overlappingB, overlappingA));

    Pairs.IntPair enclosedA = new Pairs.IntPair(0, 20);
    Pairs.IntPair enclosedB = new Pairs.IntPair(5, 15);
    assertTrue(IntRanges.rangesAreMergeable(enclosedA, enclosedB));
    assertTrue(IntRanges.rangesAreMergeable(enclosedB, enclosedA));
  }

  @Test
  public void testMergeIntoFirst() {
    Pairs.IntPair a = new Pairs.IntPair(0, 10);
    Pairs.IntPair b = new Pairs.IntPair(11, 20);
    IntRanges.mergeIntoFirst(a, b);

    // a should contain the merged interval
    assertEquals(a.getLeft(), 0);
    assertEquals(a.getRight(), 20);

    // b should be unchanged
    assertEquals(b.getLeft(), 11);
    assertEquals(b.getRight(), 20);
  }
}
