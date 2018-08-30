/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.filter.BaseFilterOperator;
import com.linkedin.pinot.core.operator.filter.OrFilterOperator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OrFilterOperatorTest {

  @Test
  public void testUnionForTwoLists() {
    int[] docIds1 = new int[]{2, 3, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    TreeSet<Integer> treeSet = new TreeSet<>();
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds1)));
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds2)));
    Iterator<Integer> expectedIterator = treeSet.iterator();

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds1));
    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds2));
    OrFilterOperator orOperator = new OrFilterOperator(operators);

    BlockDocIdIterator iterator = orOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      Assert.assertEquals(docId, expectedIterator.next().intValue());
    }
  }

  @Test
  public void testUnionForThreeLists() {
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};
    TreeSet<Integer> treeSet = new TreeSet<>();
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds1)));
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds2)));
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds3)));
    Iterator<Integer> expectedIterator = treeSet.iterator();

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds1));
    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds2));
    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds3));
    OrFilterOperator orOperator = new OrFilterOperator(operators);

    BlockDocIdIterator iterator = orOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      Assert.assertEquals(docId, expectedIterator.next().intValue());
    }
  }

  @Test
  public void testComplex() {
    int[] docIds1 = new int[]{2, 3, 6, 10, 15, 16, 28};
    int[] docIds2 = new int[]{3, 6, 8, 20, 28};
    int[] docIds3 = new int[]{1, 2, 3, 6, 30};
    TreeSet<Integer> treeSet = new TreeSet<>();
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds1)));
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds2)));
    treeSet.addAll(Arrays.asList(ArrayUtils.toObject(docIds3)));
    Iterator<Integer> expectedIterator = treeSet.iterator();

    List<BaseFilterOperator> childOperators = new ArrayList<>();
    childOperators.add(FilterOperatorTestUtils.makeFilterOperator(docIds1));
    childOperators.add(FilterOperatorTestUtils.makeFilterOperator(docIds2));
    OrFilterOperator childOrOperator = new OrFilterOperator(childOperators);

    List<BaseFilterOperator> operators = new ArrayList<>();
    operators.add(childOrOperator);
    operators.add(FilterOperatorTestUtils.makeFilterOperator(docIds3));
    OrFilterOperator orOperator = new OrFilterOperator(operators);

    BlockDocIdIterator iterator = orOperator.nextBlock().getBlockDocIdSet().iterator();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      Assert.assertEquals(docId, expectedIterator.next().intValue());
    }
  }
}
