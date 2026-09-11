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
package org.apache.pinot.core.operator.filter;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class NotFilterOperatorTest {

  @Test
  public void testNotOperator() {
    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 17, 18, 21, 22, 23, 24, 26, 28};
    List<Integer> expectedResult = Arrays.asList(0, 1, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 19, 20, 25, 27, 29);
    Iterator<Integer> expectedIterator = expectedResult.iterator();
    NotFilterOperator notFilterOperator = new NotFilterOperator(new TestFilterOperator(docIds1, 30), 30, false);
    BlockDocIdIterator iterator = notFilterOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      assertEquals(docId, expectedIterator.next().intValue());
    }
  }

  @Test
  public void testNotWithNull() {
    int numDocs = 10;
    int[] docIds = new int[]{0, 1, 2, 3};
    int[] nullDocIds = new int[]{4, 5, 6};

    NotFilterOperator notFilterOperator =
        new NotFilterOperator(new TestFilterOperator(docIds, nullDocIds, numDocs), numDocs, true);

    assertEquals(TestUtils.getDocIds(notFilterOperator.getTrues()), List.of(7, 8, 9));
    assertEquals(TestUtils.getDocIds(notFilterOperator.getFalses()), List.of(0, 1, 2, 3));
  }

  @Test
  public void testNotPropagatesNulls() {
    int numDocs = 6;
    NotFilterOperator notFilterOperator =
        new NotFilterOperator(new TestFilterOperator(new int[]{0, 1}, new int[]{2, 3}, numDocs), numDocs, true);
    assertTrue(notFilterOperator.mayHaveNulls());
    assertEquals(TestUtils.getDocIds(notFilterOperator.getNulls()), List.of(2, 3));

    // A parent reads the nulls of its children when deriving its falses: doc 2 is UNKNOWN through the negation while
    // the other child is true, so it is UNKNOWN for the AND and stays out of the outer negation
    AndFilterOperator andFilterOperator =
        new AndFilterOperator(List.of(notFilterOperator, new TestFilterOperator(new int[]{0, 2, 4}, numDocs)), null,
            numDocs, true);
    NotFilterOperator outerNotFilterOperator = new NotFilterOperator(andFilterOperator, numDocs, true);
    assertEquals(TestUtils.getDocIds(outerNotFilterOperator.getTrues()), List.of(0, 1, 3, 5));
  }

  @Test
  public void testNotCountAndBitmapsWithNull() {
    int numDocs = 10;
    ImmutableRoaringBitmap docIds = ImmutableRoaringBitmap.bitmapOf(0, 1, 2, 3);
    ImmutableRoaringBitmap nullBitmap = ImmutableRoaringBitmap.bitmapOf(4, 5, 6);

    BitmapBasedFilterOperator child = new BitmapBasedFilterOperator(docIds, false, numDocs, nullBitmap);
    assertTrue(child.canOptimizeCount());
    assertEquals(child.getNumMatchingDocs(), 4);
    assertEquals(child.getBitmaps().reduce().toArray(), new int[]{0, 1, 2, 3});

    NotFilterOperator notFilterOperator = new NotFilterOperator(child, numDocs, true);
    assertTrue(notFilterOperator.mayHaveNulls());
    assertTrue(notFilterOperator.canOptimizeCount());
    assertEquals(notFilterOperator.getNumMatchingDocs(), 3);
    assertTrue(notFilterOperator.canProduceBitmaps());
    assertEquals(notFilterOperator.getBitmaps().reduce().toArray(), new int[]{7, 8, 9});
    assertEquals(TestUtils.getDocIds(notFilterOperator.getTrues()), List.of(7, 8, 9));
  }

  @Test
  public void testNotCountAndBitmapsWithNullOnExclusiveChild() {
    int numDocs = 10;
    // The child is true on every document but {0, 1, 2, 3}, of which {2, 3} are UNKNOWN rather than false
    ImmutableRoaringBitmap docIds = ImmutableRoaringBitmap.bitmapOf(0, 1, 2, 3);
    ImmutableRoaringBitmap nullBitmap = ImmutableRoaringBitmap.bitmapOf(2, 3);

    BitmapBasedFilterOperator child = new BitmapBasedFilterOperator(docIds, true, numDocs, nullBitmap);
    assertEquals(child.getNumMatchingDocs(), 6);
    assertEquals(child.getBitmaps().reduce().toArray(), new int[]{4, 5, 6, 7, 8, 9});

    NotFilterOperator notFilterOperator = new NotFilterOperator(child, numDocs, true);
    assertEquals(notFilterOperator.getNumMatchingDocs(), 2);
    assertEquals(notFilterOperator.getBitmaps().reduce().toArray(), new int[]{0, 1});
    assertEquals(TestUtils.getDocIds(notFilterOperator.getTrues()), List.of(0, 1));
  }

  @Test
  public void testNotCountWithoutNull() {
    int numDocs = 10;
    NotFilterOperator notFilterOperator = new NotFilterOperator(
        new BitmapBasedFilterOperator(ImmutableRoaringBitmap.bitmapOf(0, 1, 2, 3), false, numDocs), numDocs, true);

    assertFalse(notFilterOperator.mayHaveNulls());
    assertTrue(notFilterOperator.canOptimizeCount());
    assertEquals(notFilterOperator.getNumMatchingDocs(), 6);
  }

  @Test
  public void testNotEmptyFilterOperator() {
    int numDocs = 5;

    NotFilterOperator notFilterOperator = new NotFilterOperator(EmptyFilterOperator.getInstance(), numDocs, true);

    assertEquals(TestUtils.getDocIds(notFilterOperator.getTrues()), List.of(0, 1, 2, 3, 4));
    assertEquals(TestUtils.getDocIds(notFilterOperator.getFalses()), List.of());
  }
}
