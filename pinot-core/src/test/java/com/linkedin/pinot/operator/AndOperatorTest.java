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
import java.util.List;

import org.testng.annotations.Test;

import com.linkedin.pinot.core.common.BaseFilterBlock;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockDocIdSet;
import com.linkedin.pinot.core.common.BlockId;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.FilterBlockDocIdSet;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.operator.filter.AndOperator;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.OrOperator;


public class AndOperatorTest {

  @Test
  public void testIntersectionForTwoLists() {
    int[] list1 = new int[] { 2, 3, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    List<Operator> operators = new ArrayList<Operator>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));
    final AndOperator andOperator = new AndOperator(operators);

    andOperator.open();
    BaseFilterBlock block;
    while ((block = andOperator.getNextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        System.out.println(docId);
      }
    }
    andOperator.close();
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

    final AndOperator andOperator = new AndOperator(operators);

    andOperator.open();
    BaseFilterBlock block;
    while ((block = andOperator.getNextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        System.out.println(docId);
      }
    }
    andOperator.close();
  }

  @Test
  public void testComplex() {
    int[] list1 = new int[] { 2, 3, 6, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    int[] list3 = new int[] { 1, 2, 3, 6, 30 };

    List<Operator> operators = new ArrayList<Operator>();
    operators.add(makeFilterOperator(list1));
    operators.add(makeFilterOperator(list2));

    final AndOperator andOperator1 = new AndOperator(operators);
    List<Operator> operators1 = new ArrayList<Operator>();
    operators1.add(andOperator1);
    operators1.add(makeFilterOperator(list3));

    final AndOperator andOperator = new AndOperator(operators1);

    andOperator.open();
    BaseFilterBlock block;
    while ((block = andOperator.getNextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        System.out.println(docId);
      }
    }
    andOperator.close();
  }

  @Test
  public void testComplexWithOr() {
    int[] list1 = new int[] { 2, 3, 6, 10, 15, 16, 28 };
    int[] list2 = new int[] { 3, 6, 8, 20, 28 };
    int[] list3 = new int[] { 1, 2, 3, 6, 30 };

    List<Operator> operators = new ArrayList<Operator>();
    operators.add(makeFilterOperator(list3));
    operators.add(makeFilterOperator(list2));

    final OrOperator orOperator = new OrOperator(operators);
    List<Operator> operators1 = new ArrayList<Operator>();
    operators1.add(orOperator);
    operators1.add(makeFilterOperator(list1));

    final AndOperator andOperator = new AndOperator(operators1);

    andOperator.open();
    BaseFilterBlock block;
    while ((block = andOperator.getNextBlock()) != null) {
      final BlockDocIdSet blockDocIdSet = block.getBlockDocIdSet();
      final BlockDocIdIterator iterator = blockDocIdSet.iterator();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        System.out.println(docId);
      }
    }
    andOperator.close();
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
                    return counter;
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
