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
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.docidsets.EmptyDocIdSet;
import org.apache.pinot.core.operator.docidsets.MatchAllDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class OrFilterOperatorTest {

  @Test
  public void testUnionForTwoLists() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    TreeSet<Integer> treeSet = new TreeSet<>();
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds1)));
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds2)));
    Iterator<Integer> expectedIterator = treeSet.iterator();

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(new TestFilterOperator(docIds1, numDocs));
    operators.add(new TestFilterOperator(docIds2, numDocs));
    OrFilterOperator orOperator = new OrFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = orOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      assertEquals(docId, expectedIterator.next().intValue());
    }
    assertFalse(expectedIterator.hasNext());
  }

  @Test
  public void testUnionForThreeLists() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};
    TreeSet<Integer> treeSet = new TreeSet<>();
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds1)));
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds2)));
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds3)));
    Iterator<Integer> expectedIterator = treeSet.iterator();

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(new TestFilterOperator(docIds1, numDocs));
    operators.add(new TestFilterOperator(docIds2, numDocs));
    operators.add(new TestFilterOperator(docIds3, numDocs));
    OrFilterOperator orOperator = new OrFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = orOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      assertEquals(docId, expectedIterator.next().intValue());
    }
    assertFalse(expectedIterator.hasNext());
  }

  @Test
  public void testComplex() {
    int numDocs = 40;
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};
    TreeSet<Integer> treeSet = new TreeSet<>();
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds1)));
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds2)));
    treeSet.addAll(List.of(ArrayUtils.toObject(docIds3)));
    Iterator<Integer> expectedIterator = treeSet.iterator();

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    childOperators.add(new TestFilterOperator(docIds1, numDocs));
    childOperators.add(new TestFilterOperator(docIds2, numDocs));
    OrFilterOperator childOrOperator = new OrFilterOperator(childOperators, null, numDocs, false);

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(childOrOperator);
    operators.add(new TestFilterOperator(docIds3, numDocs));
    OrFilterOperator orOperator = new OrFilterOperator(operators, null, numDocs, false);

    BlockDocIdIterator iterator = orOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      assertEquals(docId, expectedIterator.next().intValue());
    }
    assertFalse(expectedIterator.hasNext());
  }

  @Test
  public void testOrWithNull() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] docIds2 = new int[]{0, 1, 2};
    int[] nullDocIds1 = new int[]{4, 5, 6};
    int[] nullDocIds2 = new int[]{3, 4, 5, 6, 7};

    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs),
            new TestFilterOperator(docIds2, nullDocIds2, numDocs)), null, numDocs, true);

    assertEquals(TestUtils.getDocIds(orFilterOperator.getTrues()), List.of(0, 1, 2, 3));
    assertEquals(TestUtils.getDocIds(orFilterOperator.getFalses()), List.of(8, 9));
  }

  @Test
  public void testOrWithNullHandlingDisabled() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] docIds2 = new int[]{0, 1, 2};
    int[] nullDocIds1 = new int[]{};
    int[] nullDocIds2 = new int[]{};

    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs),
            new TestFilterOperator(docIds2, nullDocIds2, numDocs)), null, numDocs, false);

    assertEquals(TestUtils.getDocIds(orFilterOperator.getTrues()), List.of(0, 1, 2, 3));
    assertEquals(TestUtils.getDocIds(orFilterOperator.getFalses()), List.of(4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testOrWithNullOneFilterIsEmpty() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] nullDocIds1 = new int[]{4, 5, 6};

    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs), EmptyFilterOperator.getInstance()), null,
        numDocs, true);

    assertEquals(TestUtils.getDocIds(orFilterOperator.getTrues()), List.of(1, 2, 3));
    assertEquals(TestUtils.getDocIds(orFilterOperator.getFalses()), List.of(0, 7, 8, 9));
  }

  @Test
  public void testOrWithNullOneFilterMatchesAll() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] nullDocIds1 = new int[]{4, 5, 6};

    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(new TestFilterOperator(docIds1, nullDocIds1, numDocs), new MatchAllFilterOperator(numDocs)), null,
        numDocs, true);

    assertEquals(TestUtils.getDocIds(orFilterOperator.getTrues()), List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
    assertEquals(TestUtils.getDocIds(orFilterOperator.getFalses()), List.of());
  }

  @Test
  public void testOrWithAllEmpty() {
    int numDocs = 10;

    OrFilterOperator orFilterOperator =
        new OrFilterOperator(List.of(EmptyFilterOperator.getInstance(), EmptyFilterOperator.getInstance()), null,
            numDocs, true);

    assertEquals(TestUtils.getDocIds(orFilterOperator.getTrues()), List.of());
    assertEquals(TestUtils.getDocIds(orFilterOperator.getFalses()), List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }

  @Test
  public void testOrWithMatchAllFilterEarlyTermination() {
    int numDocs = 10;
    int[] regularDocIds = new int[]{1, 2, 3};

    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(
            new TestFilterOperator(regularDocIds, numDocs),
            new MatchAllFilterOperator(numDocs)
        ), null, numDocs, false);

    assertTrue((orFilterOperator.getTrues()).getOptimizedDocIdSet() instanceof MatchAllDocIdSet);
  }

  @Test
  public void testOrWithOnlyEmptyFilterEarlyTermination() {
    int numDocs = 10;
    int[] emptyDocIds = new int[0];

    OrFilterOperator orFilterOperator = new OrFilterOperator(
        List.of(
            new TestFilterOperator(emptyDocIds, numDocs),
            new TestFilterOperator(emptyDocIds, numDocs)
        ), null, numDocs, false);

    assertTrue(orFilterOperator.getTrues().getOptimizedDocIdSet() instanceof EmptyDocIdSet);
  }

  @Test
  public void testCanProduceBitmapsWhenAllChildrenCan() {
    int numDocs = 40;
    OrFilterOperator orOperator = new OrFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 10), bitmapOp(numDocs, false, 3, 10, 20)), null, numDocs, false);
    assertTrue(orOperator.canProduceBitmaps());
    assertTrue(orOperator.canOptimizeCount());
  }

  @Test
  public void testCannotProduceBitmapsWhenAnyChildCannot() {
    int numDocs = 40;
    OrFilterOperator orOperator = new OrFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 10), new TestFilterOperator(new int[]{3, 10, 20}, numDocs)), null,
        numDocs, false);
    assertFalse(orOperator.canProduceBitmaps());
    assertFalse(orOperator.canOptimizeCount());
  }

  @Test
  public void testGetBitmapsUnionForTwoChildren() {
    int numDocs = 40;
    OrFilterOperator orOperator = new OrFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 10, 15, 16, 28), bitmapOp(numDocs, false, 3, 6, 8, 20, 28)), null,
        numDocs, false);
    assertEquals(orOperator.getBitmaps().reduce().toArray(), new int[]{2, 3, 6, 8, 10, 15, 16, 20, 28});
  }

  @Test
  public void testGetBitmapsUnionForThreeChildren() {
    int numDocs = 40;
    OrFilterOperator orOperator = new OrFilterOperator(
        List.of(bitmapOp(numDocs, false, 2, 3, 6), bitmapOp(numDocs, false, 3, 6, 8), bitmapOp(numDocs, false, 1)),
        null, numDocs, false);
    assertEquals(orOperator.getBitmaps().reduce().toArray(), new int[]{1, 2, 3, 6, 8});
  }

  @Test
  public void testGetBitmapsWithExclusiveChild() {
    int numDocs = 10;
    // Second child is exclusive: it matches every doc except {0..7}, i.e. {8, 9}, so reduce() must materialize its
    // complement before the union. The OR is {1, 3} union {8, 9}.
    OrFilterOperator orOperator = new OrFilterOperator(
        List.of(bitmapOp(numDocs, false, 1, 3), bitmapOp(numDocs, true, 0, 1, 2, 3, 4, 5, 6, 7)), null, numDocs,
        false);
    assertEquals(orOperator.getBitmaps().reduce().toArray(), new int[]{1, 3, 8, 9});
  }

  @Test
  public void testGetBitmapsWithNestedOr() {
    int numDocs = 40;
    OrFilterOperator childOr =
        new OrFilterOperator(List.of(bitmapOp(numDocs, false, 2, 3), bitmapOp(numDocs, false, 6, 8)), null,
            numDocs, false);
    OrFilterOperator orOperator =
        new OrFilterOperator(List.of(childOr, bitmapOp(numDocs, false, 1, 30)), null, numDocs, false);
    assertTrue(orOperator.canProduceBitmaps());
    assertEquals(orOperator.getBitmaps().reduce().toArray(), new int[]{1, 2, 3, 6, 8, 30});
  }

  private static BitmapBasedFilterOperator bitmapOp(int numDocs, boolean exclusive, int... docIds) {
    MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
    bitmap.add(docIds);
    return new BitmapBasedFilterOperator(bitmap.toImmutableRoaringBitmap(), exclusive, numDocs);
  }
}
