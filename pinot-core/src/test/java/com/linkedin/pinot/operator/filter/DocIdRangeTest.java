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

import com.linkedin.pinot.common.utils.DocIdRange;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for DocIdRanges.
 */
public class DocIdRangeTest {
  @Test
  public void testClip() {
    DocIdRange range = new DocIdRange(10, 20);
    DocIdRange clipRange = new DocIdRange(0, 100);
    range.clip(clipRange);

    assertEquals(range.getStart(), 10);
    assertEquals(range.getEnd(), 20);

    clipRange = new DocIdRange(0, 18);
    range.clip(clipRange);
    assertEquals(range.getStart(), 10);
    assertEquals(range.getEnd(), 18);

    clipRange = new DocIdRange(12, 30);
    range.clip(clipRange);
    assertEquals(range.getStart(), 12);
    assertEquals(range.getEnd(), 18);

    clipRange = new DocIdRange(14, 16);
    range.clip(clipRange);
    assertEquals(range.getStart(), 14);
    assertEquals(range.getEnd(), 16);
  }

  @Test
  public void testIsDegenerate() {
    DocIdRange valid = new DocIdRange(10, 20);
    assertFalse(valid.isInvalid());

    valid = new DocIdRange(15, 15);
    assertFalse(valid.isInvalid());

    DocIdRange invalid = new DocIdRange(15, 14);
    assertTrue(invalid.isInvalid());
  }

  @Test
  public void testRangesAreMergeable() {
    DocIdRange disjointA = new DocIdRange(0, 10);
    DocIdRange disjointB = new DocIdRange(12, 20);
    assertFalse(disjointA.rangeIsMergeable(disjointB));
    assertFalse(disjointB.rangeIsMergeable(disjointA));

    DocIdRange adjacentA = new DocIdRange(0, 10);
    DocIdRange adjacentB = new DocIdRange(11, 20);
    assertTrue(adjacentA.rangeIsMergeable(adjacentB));
    assertTrue(adjacentB.rangeIsMergeable(adjacentA));

    DocIdRange overlappingA = new DocIdRange(0, 10);
    DocIdRange overlappingB = new DocIdRange(10, 15);
    assertTrue(overlappingA.rangeIsMergeable(overlappingB));
    assertTrue(overlappingB.rangeIsMergeable(overlappingA));

    DocIdRange enclosedA = new DocIdRange(0, 20);
    DocIdRange enclosedB = new DocIdRange(5, 15);
    assertTrue(enclosedA.rangeIsMergeable(enclosedB));
    assertTrue(enclosedB.rangeIsMergeable(enclosedA));
  }

  @Test
  public void testMergeIntoFirst() {
    DocIdRange a = new DocIdRange(0, 10);
    DocIdRange b = new DocIdRange(11, 20);
    a.mergeWithRange(b);

    // a should contain the merged interval
    assertEquals(a.getStart(), 0);
    assertEquals(a.getEnd(), 20);

    // b should be unchanged
    assertEquals(b.getStart(), 11);
    assertEquals(b.getEnd(), 20);
  }
}
