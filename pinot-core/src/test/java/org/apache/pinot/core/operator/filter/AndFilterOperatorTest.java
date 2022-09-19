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
import org.apache.pinot.segment.spi.Constants;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.Test;


public class AndFilterOperatorTest {

  @Test
  public void testIntersectionForTwoLists() {
    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(new TestFilterOperator(docIds1));
    operators.add(new TestFilterOperator(docIds2));
    AndFilterOperator andOperator = new AndFilterOperator(operators);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 28);
    Assert.assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testIntersectionForThreeLists() {
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(new TestFilterOperator(docIds1));
    operators.add(new TestFilterOperator(docIds2));
    operators.add(new TestFilterOperator(docIds3));
    AndFilterOperator andOperator = new AndFilterOperator(operators);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 6);
    Assert.assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testComplex() {
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    childOperators.add(new TestFilterOperator(docIds1));
    childOperators.add(new TestFilterOperator(docIds2));
    AndFilterOperator childAndOperator = new AndFilterOperator(childOperators);

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(childAndOperator);
    operators.add(new TestFilterOperator(docIds3));
    AndFilterOperator andOperator = new AndFilterOperator(operators);

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

    AndFilterOperator andFilterOperator1 = new AndFilterOperator(childOperators1);
    AndFilterOperator andFilterOperator2 = new AndFilterOperator(childOperators2);
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
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    childOperators.add(new TestFilterOperator(docIds3));
    childOperators.add(new TestFilterOperator(docIds2));
    OrFilterOperator childOrOperator = new OrFilterOperator(childOperators, 40);

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(childOrOperator);
    operators.add(new TestFilterOperator(docIds1));
    AndFilterOperator andOperator = new AndFilterOperator(operators);

    BlockDocIdIterator iterator = andOperator.nextBlock().getBlockDocIdSet().iterator();
    Assert.assertEquals(iterator.next(), 2);
    Assert.assertEquals(iterator.next(), 3);
    Assert.assertEquals(iterator.next(), 6);
    Assert.assertEquals(iterator.next(), 28);
    Assert.assertEquals(iterator.next(), Constants.EOF);
  }
}
