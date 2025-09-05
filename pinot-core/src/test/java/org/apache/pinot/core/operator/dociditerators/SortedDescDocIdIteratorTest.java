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
package org.apache.pinot.core.operator.dociditerators;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.docidsets.SortedDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.Pairs;
import org.assertj.core.api.Assertions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class SortedDescDocIdIteratorTest {

  @DataProvider
  public Object[][] provideDocIdRanges() {
    return new Object[][]{
        {Collections.singletonList(new Pairs.IntPair(1, 1)), Collections.singletonList(1)},
        {Collections.singletonList(new Pairs.IntPair(5, 15)),
            Arrays.asList(15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5)},
        {Arrays.asList(new Pairs.IntPair(20, 25), new Pairs.IntPair(30, 35)),
            Arrays.asList(35, 34, 33, 32, 31, 30, 25, 24, 23, 22, 21, 20)}
    };
  }

  /// Iterates in descending order, one by one, and verifies the results.
  @Test(dataProvider = "provideDocIdRanges")
  public void testDenseIteration(List<Pairs.IntPair> docIdRanges, List<Integer> expectedDocIds) {
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      List<Integer> result = new ArrayList<>();
      int docId;
      while ((docId = iterator.next()) != Constants.EOF) {
        result.add(docId);
      }
      assertEquals(result, expectedDocIds);
    }
  }

  /// Iterates in descending order, using advance() to the next expected docId, and verifies the results.
  @Test(dataProvider = "provideDocIdRanges")
  public void testAdvanceDense(List<Pairs.IntPair> docIdRanges, List<Integer> expectedDocIds) {
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      for (Integer expectedDocId : expectedDocIds) {
        int actualDocId = iterator.advance(expectedDocId);
        assertEquals(actualDocId, expectedDocId.intValue());
      }
      assertEquals(iterator.next(), Constants.EOF);
    }
  }

  /// Iterates in descending order, using advance() into gaps, and verifies the results.
  @Test(dataProvider = "provideDocIdRanges")
  public void testAdvanceIntoGap(List<Pairs.IntPair> docIdRanges, List<Integer> expectedDocIds) {
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      for (Pairs.IntPair intPair : Lists.reverse(docIdRanges)) {
        int prevDoc = intPair.getLeft() - 1;
        int next = iterator.advance(prevDoc);
        if (next != Constants.EOF) {
          Assertions.assertThat(next).isLessThan(prevDoc);
        }
      }
    }
  }
}
