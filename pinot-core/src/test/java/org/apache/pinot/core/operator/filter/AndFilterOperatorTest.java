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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


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
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 28);
    Assert.assertEquals(iterator.next(), Constants.EOF);
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
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 6);
    Assert.assertEquals(iterator.next(), Constants.EOF);
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
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 6);
    Assert.assertEquals(iterator.next(), Constants.EOF);
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
    Assert.assertEquals(iterator1.next(), 0);
    Assert.assertEquals(iterator1.next(), 60);
    Assert.assertEquals(iterator1.next(), 120);
    Assert.assertEquals(iterator1.next(), 180);

    Assert.assertEquals(iterator2.next(), 0);
    Assert.assertEquals(iterator2.next(), 60);
    Assert.assertEquals(iterator2.next(), 120);
    Assert.assertEquals(iterator2.next(), 180);

    for (int i = 0; i < numDocs / 10; i++) {
      Assert.assertEquals(iterator1.next(), iterator2.next());
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
    Assert.assertEquals(iterator.next(), 2);
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 6);
    Assert.assertEquals(iterator.next(), 28);
    Assert.assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAndWithNull() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] docIds2 = new int[]{0, 1, 2};
    int[] nullDocIds1 = new int[]{4, 5, 6};
    int[] nullDocIds2 = new int[]{3, 4, 5, 6, 7};

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        Arrays.asList(new TestFilterOperator(docIds1, nullDocIds1, numDocs),
            new TestFilterOperator(docIds2, nullDocIds2, numDocs)), null, numDocs, true);

    Assert.assertEquals(TestUtils.getDocIds(andFilterOperator.getNextBlock().getBlockDocIdSet()), List.of(1, 2));
    Assert.assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(0, 7, 8, 9));
  }

  @Test
  public void testAndWithNullOneFilterIsEmpty() {
    int numDocs = 10;
    int[] docIds1 = new int[]{1, 2, 3};
    int[] nullDocIds1 = new int[]{4, 5, 6};

    AndFilterOperator andFilterOperator = new AndFilterOperator(
        Arrays.asList(new TestFilterOperator(docIds1, nullDocIds1, numDocs), EmptyFilterOperator.getInstance()), null,
        numDocs, true);

    Assert.assertEquals(TestUtils.getDocIds(andFilterOperator.getNextBlock().getBlockDocIdSet()),
        Collections.emptyList());
    Assert.assertEquals(TestUtils.getDocIds(andFilterOperator.getFalses()), List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
  }
}
