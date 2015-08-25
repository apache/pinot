/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import junit.framework.Assert;

import org.apache.commons.lang3.ArrayUtils;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.OrOperator;


public class OrOperatorTest {

  @Test
  public void testIntersectionForTwoLists() {
    int[] list1 = new int[] { 2, 3, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    ;
    List<Operator> operators = new ArrayList<Operator>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));
    final OrOperator orOperator = new OrOperator(operators);

    orOperator.open();
    Block block;
    TreeSet<Integer> set = new TreeSet<Integer>();
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list1)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list2)));
    Iterator<Integer> expectedIterator = set.iterator();
    while ((block = orOperator.nextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        Assert.assertEquals(expectedIterator.next().intValue(), docId);
      }
    }
    orOperator.close();
  }

  @Test
  public void testIntersectionForThreeLists() {
    int[] list1 = new int[] { 2, 3, 6, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    int[] list3 = new int[] { 1, 2, 3, 6, 30 };

    List<Operator> operators = new ArrayList<Operator>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));
    operators.add(makeFilterOperator(list3));

    final OrOperator orOperator = new OrOperator(operators);

    orOperator.open();
    Block block;
    TreeSet<Integer> set = new TreeSet<Integer>();
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list1)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list2)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list3)));
    Iterator<Integer> expectedIterator = set.iterator();
    while ((block = orOperator.nextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        Assert.assertEquals(expectedIterator.next().intValue(), docId);
      }
    }
    orOperator.close();
  }

  @Test
  public void testComplex() {
    int[] list1 = new int[] { 2, 3, 6, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    int[] list3 = new int[] { 1, 2, 3, 6, 30 };

    TreeSet<Integer> set = new TreeSet<Integer>();
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list1)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list2)));
    set.addAll(Lists.newArrayList(ArrayUtils.toObject(list3)));
    Iterator<Integer> expectedIterator = set.iterator();

    List<Operator> operators = new ArrayList<Operator>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));

    final OrOperator orOperator1 = new OrOperator(operators);
    List<Operator> operators1 = new ArrayList<Operator>();
    operators1.add(orOperator1);
    operators1.add(makeFilterOperator(list3));

    final OrOperator orOperator = new OrOperator(operators1);

    orOperator.open();
    BaseFilterBlock block;
    while ((block = orOperator.getNextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        System.out.println(docId);
        Assert.assertEquals(expectedIterator.next().intValue(), docId);
      }
    }
    orOperator.close();
  }

  public BaseFilterOperator makeFilterOperator(final int[] list) {
    return new BaseFilterOperator() {
      boolean alreadyInvoked = false;

      @Override
      public boolean open() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public boolean close() {
        // TODO Auto-generated method stub
        return false;
      }

      @Override
      public BaseFilterBlock nextFilterBlock(BlockId blockId) {

        if (alreadyInvoked) {
          return null;
        }
        alreadyInvoked = true;

        return new BaseFilterBlock() {

          @Override
          public BlockId getId() {
            // TODO Auto-generated method stub
            return null;
          }

          @Override
          public FilterBlockDocIdSet getFilteredBlockDocIdSet() {
            return new FilterBlockDocIdSet() {

              @Override
              public BlockDocIdIterator iterator() {

                return new BlockDocIdIterator() {
                  int counter = 0;

                  @Override
                  public int advance(int targetDocId) {
                    int ret = Constants.EOF;
                    while (counter < list.length) {
                      if (list[counter] >= targetDocId) {
                        ret = list[counter];
                        break;
                      }
                      counter = counter + 1;
                    }
                    return ret;
                  }

                  @Override
                  public int next() {
                    if (counter == list.length) {
                      return Constants.EOF;
                    }
                    int ret = list[counter];
                    counter = counter + 1;
                    return ret;
                  }

                  @Override
                  public int currentDocId() {
                    return 0;
                  }
                };
              }

              @Override
              public <T> T getRaw() {
                // TODO Auto-generated method stub
                return null;
              }

              @Override
              public void setStartDocId(int startDocId) {
                // TODO Auto-generated method stub

              }

              @Override
              public void setEndDocId(int endDocId) {
                // TODO Auto-generated method stub

              }

              @Override
              public int getMinDocId() {
                return list[0];
              }

              @Override
              public int getMaxDocId() {
                return list[list.length - 1];
              }
            };
          }
        };
      }
    };
  }
}
