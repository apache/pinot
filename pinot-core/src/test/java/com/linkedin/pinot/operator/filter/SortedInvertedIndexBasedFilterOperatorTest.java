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
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.io.reader.impl.v1.SortedIndexReader;
import com.linkedin.pinot.core.operator.blocks.BaseFilterBlock;
import com.linkedin.pinot.core.operator.docidsets.FilterBlockDocIdSet;
import com.linkedin.pinot.core.operator.filter.SortedInvertedIndexBasedFilterOperator;
import com.linkedin.pinot.core.operator.filter.predicate.PredicateEvaluator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class SortedInvertedIndexBasedFilterOperatorTest {

  @Test
  public void testSortedInvertedIndexBasedFilterOperatorForMatchingDocId() {

    DataSource ds = mock(DataSource.class);
    SortedIndexReader invertedIndexReader = mock(SortedIndexReader.class);
    doReturn(invertedIndexReader).when(ds).getInvertedIndex();

    int expectedMinDocId = 1;
    int expectedMaxDocId = 12;
    when(invertedIndexReader.getDocIds(anyInt()))
        .thenReturn(new Pairs.IntPair(expectedMinDocId, 5))
        .thenReturn(new Pairs.IntPair(8, expectedMaxDocId));

    PredicateEvaluator predicateEvaluator = spy(PredicateEvaluator.class);
    int[] dictIds = {1, 2};
    doReturn(false).when(predicateEvaluator).isExclusive();
    doReturn(dictIds).when(predicateEvaluator).getMatchingDictIds();

    int startDocId = 0;
    int endDocId = 15;
    SortedInvertedIndexBasedFilterOperator sortedInvertedIndexBasedFilterOperator =
        new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, ds, startDocId, endDocId);


    Assert.assertFalse(sortedInvertedIndexBasedFilterOperator.isResultEmpty());

    BaseFilterBlock baseFilterBlock = sortedInvertedIndexBasedFilterOperator.nextBlock();
    FilterBlockDocIdSet filterBlockDocIdSet = baseFilterBlock.getFilteredBlockDocIdSet();
    Assert.assertEquals(filterBlockDocIdSet.getMinDocId(), expectedMinDocId);
    Assert.assertEquals(filterBlockDocIdSet.getMaxDocId(), expectedMaxDocId);

    BlockDocIdIterator blockDocIdIterator = filterBlockDocIdSet.iterator();
    int num = blockDocIdIterator.next();
    List<Integer> actualDocIds = new ArrayList<>();
    List<Integer> expectedDocIds = Arrays.asList(1,2,3,4,5,8,9,10,11,12);
    while (Constants.EOF != num) {
      actualDocIds.add(num);
      num = blockDocIdIterator.next();
    }
    Assert.assertEquals(filterBlockDocIdSet.toString(), "[[1,5], [8,12]]");
    Assert.assertEquals(actualDocIds, expectedDocIds);
  }

  @Test
  public void testSortedInvertedIndexBasedFilterOperatorForNonMatchingDocId() {
    DataSource ds = mock(DataSource.class);
    SortedIndexReader invertedIndexReader = mock(SortedIndexReader.class);
    doReturn(invertedIndexReader).when(ds).getInvertedIndex();

    int expectedMinDocId = 0;
    int expectedMaxDocId = 15;
    when(invertedIndexReader.getDocIds(anyInt()))
        .thenReturn(new Pairs.IntPair(expectedMinDocId + 1, 5))
        .thenReturn(new Pairs.IntPair(8, expectedMaxDocId - 3));

    PredicateEvaluator predicateEvaluator = spy(PredicateEvaluator.class);
    int[] dictIds = {1, 2};
    doReturn(true).when(predicateEvaluator).isExclusive();
    doReturn(dictIds).when(predicateEvaluator).getNonMatchingDictIds();

    int startDocId = 0;
    int endDocId = 15;
    SortedInvertedIndexBasedFilterOperator sortedInvertedIndexBasedFilterOperator =
        new SortedInvertedIndexBasedFilterOperator(predicateEvaluator, ds, startDocId, endDocId);


    Assert.assertFalse(sortedInvertedIndexBasedFilterOperator.isResultEmpty());

    BaseFilterBlock baseFilterBlock = sortedInvertedIndexBasedFilterOperator.nextBlock();
    FilterBlockDocIdSet filterBlockDocIdSet = baseFilterBlock.getFilteredBlockDocIdSet();
    Assert.assertEquals(filterBlockDocIdSet.getMinDocId(), expectedMinDocId);
    Assert.assertEquals(filterBlockDocIdSet.getMaxDocId(), expectedMaxDocId);

    BlockDocIdIterator blockDocIdIterator = filterBlockDocIdSet.iterator();
    int num = blockDocIdIterator.next();
    List<Integer> actualDocIds = new ArrayList<>();
    List<Integer> expectedDocIds = Arrays.asList(0,6,7,13,14,15);
    while (Constants.EOF != num) {
      actualDocIds.add(num);
      num = blockDocIdIterator.next();
    }
    Assert.assertEquals(filterBlockDocIdSet.toString(), "[[0,0], [6,7], [13,15]]");
    Assert.assertEquals(actualDocIds, expectedDocIds);
  }
}
