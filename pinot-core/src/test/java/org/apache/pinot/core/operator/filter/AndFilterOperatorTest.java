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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class AndFilterOperatorTest {

  @Test
  public void testIntersectionForTwoLists() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(new TestFilterOperator(docIds1, numDocs));
    operators.add(new TestFilterOperator(docIds2, numDocs));
    AndFilterOperator andOperator = new AndFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.next(), 28);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testIntersectionForThreeLists() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(new TestFilterOperator(docIds1, numDocs));
    operators.add(new TestFilterOperator(docIds2, numDocs));
    operators.add(new TestFilterOperator(docIds3, numDocs));
    AndFilterOperator andOperator = new AndFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.next(), 6);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testComplex() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    childOperators.add(new TestFilterOperator(docIds1, numDocs));
    childOperators.add(new TestFilterOperator(docIds2, numDocs));
    AndFilterOperator childAndOperator = new AndFilterOperator(childOperators, null, numDocs, false);

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(childAndOperator);
    operators.add(new TestFilterOperator(docIds3, numDocs));
    AndFilterOperator andOperator = new AndFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.next(), 6);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  void testAndDocIdSetReordering() {
    int numDocs = 10_000;
    int numFilters = 4;
    int filterStart = 2;

    MutableRoaringBitmap[] mutableRoaringBitmap = new MutableRoaringBitmap[numFilters];
    for (int i = filterStart; i < filterStart + numFilters; i++) {
      int k = i - filterStart;
      mutableRoaringBitmap[k] = new MutableRoaringBitmap();
      for (int j = 0; j < 10_000; j++) {
        if (j % i == 0) {
          mutableRoaringBitmap[k].add(j);
        }
      }
    }

    List<BaseFilterOperator> childOperators1 = new ArrayList<>();
    List<BaseFilterOperator> childOperators2 = new ArrayList<>();
    for (int i = 0; i < numFilters; i++) {
      childOperators1.add(
          new BitmapBasedFilterOperator(mutableRoaringBitmap[i].toImmutableRoaringBitmap(), false, numDocs));
      childOperators2.add(
          new BitmapBasedFilterOperator(mutableRoaringBitmap[numFilters - 1 - i].toImmutableRoaringBitmap(), false,
              numDocs));
    }

    AndFilterOperator andFilterOperator1 = new AndFilterOperator(childOperators1, null, numDocs, false);
    AndFilterOperator andFilterOperator2 = new AndFilterOperator(childOperators2, null, numDocs, false);
    BlockDocIdIterator iterator1 = andFilterOperator1.getNextBlock().getBlockDocIdSet().iterator();
    BlockDocIdIterator iterator2 = andFilterOperator2.getNextBlock().getBlockDocIdSet().iterator();
    assertEquals(iterator1.next(), 0);
    assertEquals(iterator1.next(), 60);
    assertEquals(iterator1.next(), 120);
    assertEquals(iterator1.next(), 180);

    assertEquals(iterator2.next(), 0);
    assertEquals(iterator2.next(), 60);
    assertEquals(iterator2.next(), 120);
    assertEquals(iterator2.next(), 180);

    for (int i = 0; i < numDocs / 10; i++) {
      assertEquals(iterator1.next(), iterator2.next());
    }
  }

  @Test
  public void testComplexWithOr() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    childOperators.add(new TestFilterOperator(docIds3, numDocs));
    childOperators.add(new TestFilterOperator(docIds2, numDocs));
    OrFilterOperator childOrOperator = new OrFilterOperator(childOperators, null, numDocs, false);

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(childOrOperator);
    operators.add(new TestFilterOperator(docIds1, numDocs));
    AndFilterOperator andOperator = new AndFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    assertEquals(iterator.next(), 2);
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.next(), 6);
    assertEquals(iterator.next(), 28);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAndWithNull() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] docIds2 = new int[]{0, 1, 2};
    int[] nullDocIds1 = new int[]{4, 5, 6};
    int[] nullDocIds2 = new int[]{3, 4, 5, 6, 7};

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs),
            new TestFilterOperator(docIds2, nullDocIds2, numDocs)), null, numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of(1, 2));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(0, 7, 8, 9));
  }

  @Test
  public void testAndWithNullHandlingDisabled() {
    int numDocs = 4;
    int[] docIds1 = new int[]{0, 3};
    int[] docIds2 = new int[]{0, 1};
    int[] nullDocIds1 = new int[]{};
    int[] nullDocIds2 = new int[]{};

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs),
            new TestFilterOperator(docIds2, nullDocIds2, numDocs)), null, numDocs, false);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of(0));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(1, 2, 3));
  }

  @Test
  public void testAndWithNullOneFilterIsEmpty() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] nullDocIds1 = new int[]{4, 5, 6};

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs), EmptyFilterOperator.getInstance()), null,
        numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of());
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()),
        List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testAndWithNullOneFilterMatchesAll() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] nullDocIds1 = new int[]{4, 5, 6};

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs), new MatchAllFilterOperator(numDocs)), null,
        numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of(1, 2, 3));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(0, 7, 8, 9));
  }

  @Test
  public void testAndWithAllMatchesAll() {
    int numDocs = 10;
    AndFilterOperator andFilterOperator =
        new AndFilterOperator(List.of(new MatchAllFilterOperator(numDocs), new MatchAllFilterOperator(numDocs)),
            null, numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()),
        List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of());
  }

  @Test
  public void testAndWithEmptyFilterEarlyTermination() {
    int numDocs = 10;
    int[] regularDocIds = new int[]{1, 2, 3};
    int[] emptyDocIds = new int[0];

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(
            new TestFilterOperator(regularDocIds, numDocs),
            new TestFilterOperator(emptyDocIds, numDocs)
        ), null, numDocs, false);

    assertEquals((andFilterOperator.getTrues()).getOptimizedDocIdSet(), EmptyDocIdSet.getInstance());
  }

  @Test
  public void testAndWithOnlyMatchAllFilterEarlyTermination() {
    int numDocs = 10;
    int numDocs2 = 50;

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(
            new MatchAllFilterOperator(numDocs),
            new MatchAllFilterOperator(numDocs2)
        ), null, numDocs, false);

    assertTrue(andFilterOperator.getTrues() instanceof MatchAllDocIdSet);
  }

  @Test
  public void testCanProduceBitmapsWhenAllChildrenCan() {
    int numDocs = 40;
    AndFilterOperator andOperator = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 10), bitmapOp(numDocs, false, 3, 10, 20)), null, numDocs, false);
    assertTrue(andOperator.canProduceBitmaps());
    assertTrue(andOperator.canOptimizeCount());
  }

  @Test
  public void testCannotProduceBitmapsWhenAnyChildCannot() {
    int numDocs = 40;
    AndFilterOperator andOperator = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 10), new TestFilterOperator(new int[]{3, 10, 20}, numDocs)), null,
        numDocs, false);
    assertFalse(andOperator.canProduceBitmaps());
    assertFalse(andOperator.canOptimizeCount());
  }

  @Test
  public void testGetBitmapsIntersectionForTwoChildren() {
    int numDocs = 40;
    AndFilterOperator andOperator = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 10, 15, 16, 28), bitmapOp(numDocs, false, 3, 6, 8, 20, 28)), null,
        numDocs, false);
    assertEquals(andOperator.getBitmaps().reduce().toArray(), new int[]{3, 28});
  }

  @Test
  public void testGetBitmapsIntersectionForThreeChildren() {
    int numDocs = 40;
    AndFilterOperator andOperator = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 6, 10, 15, 16, 28), bitmapOp(numDocs, false, 3, 6, 8, 20, 28),
            bitmapOp(numDocs, false, 1, 2, 3, 6, 30)), null, numDocs, false);
    assertEquals(andOperator.getBitmaps().reduce().toArray(), new int[]{3, 6});
  }

  @Test
  public void testGetBitmapsWithExclusiveChild() {
    int numDocs = 10;
    // Second child is exclusive: it matches every doc except {2, 4}, so reduce() must materialize its complement
    // before the intersection. The AND is {1, 2, 3, 4, 5} minus {2, 4}.
    AndFilterOperator andOperator = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 1, 2, 3, 4, 5), bitmapOp(numDocs, true, 2, 4)), null, numDocs, false);
    assertEquals(andOperator.getBitmaps().reduce().toArray(), new int[]{1, 3, 5});
  }

  @Test
  public void testGetBitmapsWithNestedAnd() {
    int numDocs = 40;
    AndFilterOperator childAnd = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 6, 28), bitmapOp(numDocs, false, 3, 6, 8, 28)), null, numDocs,
        false);
    AndFilterOperator andOperator =
        new AndFilterOperator(List.of(childAnd, bitmapOp(numDocs, false, 3, 6, 30)), null, numDocs, false);
    assertTrue(andOperator.canProduceBitmaps());
    assertEquals(andOperator.getBitmaps().reduce().toArray(), new int[]{3, 6});
  }

  @Test
  public void testGetBitmapsWithNullChild() {
    int numDocs = 10;
    // The second child is UNKNOWN on {4, 5, 6}. The AND is UNKNOWN only where no child is false: on 4, where the other
    // children are true, while 5 and 6 are false through the first child
    AndFilterOperator andOperator = new AndFilterOperator(List.of(bitmapOp(numDocs, false, 2, 3, 4, 7),
        new BitmapBasedFilterOperator(MutableRoaringBitmap.bitmapOf(0, 1, 2, 3), false, numDocs,
            MutableRoaringBitmap.bitmapOf(4, 5, 6)), bitmapOp(numDocs, false, 2, 4, 8)), null, numDocs, true);

    assertTrue(andOperator.canProduceBitmaps());
    assertTrue(andOperator.mayHaveNulls());
    BitmapCollection bitmaps = andOperator.getBitmaps();
    assertEquals(bitmaps.reduce().toArray(), new int[]{2});
    assertEquals(bitmaps.getNullBitmap().toArray(), new int[]{4});
    assertEquals(andOperator.getNumMatchingDocs(), 1);

    NotFilterOperator notOperator = new NotFilterOperator(andOperator, numDocs, true);
    assertTrue(notOperator.canOptimizeCount());
    assertEquals(notOperator.getNumMatchingDocs(), 8);
    assertEquals(notOperator.getBitmaps().reduce().toArray(), new int[]{0, 1, 3, 5, 6, 7, 8, 9});
    assertEquals(TestUtils.getDocIds(notOperator.getTrues()), List.of(0, 1, 3, 5, 6, 7, 8, 9));
  }

  @Test
  public void testGetBitmapsWithoutNullChild() {
    int numDocs = 10;
    AndFilterOperator andOperator = new AndFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 4, 7), bitmapOp(numDocs, false, 2, 4, 8)), null, numDocs, true);

    assertFalse(andOperator.mayHaveNulls());
    assertNull(andOperator.getBitmaps().getNullBitmap());
    assertEquals(andOperator.getNumMatchingDocs(), 2);
  }

  @Test
  public void testGetNulls() {
    int numDocs = 10;
    // The first child is UNKNOWN on {4, 5, 6}; the AND is UNKNOWN only on 4, where the second child is true, since 5
    // and 6 are false through it
    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(new int[]{0, 1, 2, 3}, new int[]{4, 5, 6}, numDocs),
            new TestFilterOperator(new int[]{2, 3, 4, 7}, numDocs)), null, numDocs, true);

    assertTrue(andFilterOperator.mayHaveNulls());
    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of(2, 3));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getNulls()), List.of(4));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(0, 1, 5, 6, 7, 8, 9));
  }

  @Test
  public void testChildTrueNowhereButUnknownSomewhere() {
    int numDocs = 5;
    // The first child is true nowhere and UNKNOWN on {1, 2}; the AND is UNKNOWN on 1, where the second child is
    // true, and false everywhere else
    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(new int[0], new int[]{1, 2}, numDocs),
            new TestFilterOperator(new int[]{1, 3}, numDocs)), null, numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of());
    assertEquals(TestUtils.getDocIds(andFilterOperator.getNulls()), List.of(1));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(0, 2, 3, 4));
  }

  @Test
  public void testGetNullsWhenNeverFalse() {
    int numDocs = 3;
    // The negated child is true nowhere and UNKNOWN on 1, so its negation is true on {0, 2}, UNKNOWN on 1 and false
    // nowhere. The conjunction with a match-all is then never false either, yet still UNKNOWN on 1
    NotFilterOperator notFilterOperator =
        new NotFilterOperator(new TestFilterOperator(new int[0], new int[]{1}, numDocs), numDocs, true);
    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(notFilterOperator, new MatchAllFilterOperator(numDocs)), null, numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getTrues()), List.of(0, 2));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getNulls()), List.of(1));
    assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of());
  }

  @Test
  public void testGetNullsPropagateThroughNestedOr() {
    int numDocs = 8;
    // The OR is UNKNOWN on {1, 2}, where its first child is UNKNOWN and its second false; the AND above it inherits
    // that where its own other child is true, and the outer negation has to leave those documents out
    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(new TestFilterOperator(new int[]{0}, new int[]{1, 2}, numDocs),
            new TestFilterOperator(new int[]{3}, numDocs)), null, numDocs, true);
    AndFilterOperator andFilterOperator = new AndFilterOperator(
        List.of(new TestFilterOperator(new int[]{0, 1, 2, 3, 4}, numDocs), orFilterOperator), null, numDocs, true);

    assertEquals(TestUtils.getDocIds(andFilterOperator.getNulls()), List.of(1, 2));
    NotFilterOperator notFilterOperator = new NotFilterOperator(andFilterOperator, numDocs, true);
    assertEquals(TestUtils.getDocIds(notFilterOperator.getTrues()), List.of(4, 5, 6, 7));
  }

  @Test
  public void testGetNumMatchingDocsWithNullChild() {
    int numDocs = 10;
    // The second child is exclusive, so true on all but {0, 1, 4}; the AND is true on {2, 3} and UNKNOWN on {5, 6},
    // where the first child is UNKNOWN and the second true
    AndFilterOperator andOperator = new AndFilterOperator(List.of(
        new BitmapBasedFilterOperator(MutableRoaringBitmap.bitmapOf(0, 1, 2, 3), false, numDocs,
            MutableRoaringBitmap.bitmapOf(4, 5, 6)), bitmapOp(numDocs, true, 0, 1, 4)), null, numDocs, true);

    assertTrue(andOperator.canOptimizeCount());
    assertEquals(andOperator.getNumMatchingDocs(), 2);
    BitmapCollection bitmaps = andOperator.getBitmaps();
    assertEquals(bitmaps.reduce().toArray(), new int[]{2, 3});
    assertEquals(bitmaps.getNullBitmap().toArray(), new int[]{5, 6});
  }

  private static BitmapBasedFilterOperator bitmapOp(int numDocs, boolean exclusive, int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return new BitmapBasedFilterOperator(bitmap.toImmutableRoaringBitmap(), exclusive, numDocs);
  }
}
