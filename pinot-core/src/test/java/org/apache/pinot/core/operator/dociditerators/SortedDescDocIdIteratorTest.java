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

  @Test
  public void testAdvanceToDocumentBeforeFirstRange() {
    // Test advancing to a document ID that's before all ranges
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(10, 15),
        new Pairs.IntPair(20, 25)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      // Advance to document 5, which is before all ranges
      assertEquals(iterator.advance(5), Constants.EOF);
    }
  }

  @Test
  public void testAdvanceToDocumentAfterLastRange() {
    // Test advancing to a document ID that's after all ranges
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(10, 15),
        new Pairs.IntPair(20, 25)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      // Advance to document 5, which is lower (and therefore after) all ranges
      assertEquals(iterator.advance(5), Constants.EOF);
    }
  }

  @Test
  public void testAdvanceToExactRangeBoundaries() {
    // Test advancing to exact start and end boundaries of ranges
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(10, 15),
        new Pairs.IntPair(20, 25)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      // Advance to exact start of first range
      assertEquals(iterator.advance(25), 25);
      assertEquals(iterator.next(), 24);

      // Advance to exact end of first range
      assertEquals(iterator.advance(20), 20);

      // Advance to exact start of second range
      assertEquals(iterator.advance(15), 15);

      // Advance to exact end of second range
      assertEquals(iterator.advance(10), 10);
      assertEquals(iterator.next(), Constants.EOF);
    }
  }

  @Test
  public void testAdvanceToGapBetweenRanges() {
    // Test advancing to document IDs in gaps between ranges
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(10, 15),
        new Pairs.IntPair(20, 25),
        new Pairs.IntPair(30, 35)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      // Advance to gap between first and second range
      assertEquals(iterator.advance(27), 25);

      // Advance to gap between second and third range
      assertEquals(iterator.advance(17), 15);
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEmptyRangesList() {
    // Test with empty list of ranges
    List<Pairs.IntPair> docIdRanges = Collections.emptyList();
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
    }
  }

  @Test
  public void testSingleDocumentRanges() {
    // Test with multiple single-document ranges
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(5, 5),
        new Pairs.IntPair(10, 10),
        new Pairs.IntPair(15, 15)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.next(), 15);
      assertEquals(iterator.next(), 10);
      assertEquals(iterator.next(), 5);
      assertEquals(iterator.next(), Constants.EOF);
    }
  }

  @Test
  public void testAdvanceWithinCurrentRange() {
    // Test advancing to different positions within the current range
    List<Pairs.IntPair> docIdRanges = Collections.singletonList(new Pairs.IntPair(10, 20));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.next(), 20);
      assertEquals(iterator.next(), 19);

      // Advance within current range to a lower value
      assertEquals(iterator.advance(15), 15);
      assertEquals(iterator.next(), 14);

      // Advance within current range to an even lower value
      assertEquals(iterator.advance(12), 12);
    }
  }

  @Test
  public void testLargeRangeWithManyDocuments() {
    // Test with a large range to ensure performance
    List<Pairs.IntPair> docIdRanges = Collections.singletonList(new Pairs.IntPair(1000, 2000));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.next(), 2000);
      assertEquals(iterator.advance(1500), 1500);
      assertEquals(iterator.advance(1200), 1200);
      assertEquals(iterator.next(), 1199);
    }
  }

  @Test
  public void testAdvanceWithNegativeDocId() {
    // Test advancing to negative document ID (edge case)
    List<Pairs.IntPair> docIdRanges = Collections.singletonList(new Pairs.IntPair(5, 10));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.advance(-1), Constants.EOF);
    }
  }

  @Test
  public void testAdvanceToZero() {
    // Test advancing to document ID 0
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(0, 5),
        new Pairs.IntPair(10, 15)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.advance(0), 0);
      assertEquals(iterator.next(), Constants.EOF);
    }
  }

  @Test
  public void testMixedAdvanceAndNextAfterExhaustion() {
    // Test behavior after iterator is exhausted
    List<Pairs.IntPair> docIdRanges = Collections.singletonList(new Pairs.IntPair(5, 7));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.next(), 7);
      assertEquals(iterator.next(), 6);
      assertEquals(iterator.next(), 5);
      assertEquals(iterator.next(), Constants.EOF);

      // After exhaustion, both next() and advance() should return EOF
      assertEquals(iterator.next(), Constants.EOF);
      assertEquals(iterator.advance(4), Constants.EOF);
      assertEquals(iterator.advance(3), Constants.EOF);
    }
  }

  @Test
  public void testDocIdRangeAtMaxValue() {
    // Test with document IDs near Integer.MAX_VALUE
    List<Pairs.IntPair> docIdRanges = Collections.singletonList(
        new Pairs.IntPair(Integer.MAX_VALUE - 5, Integer.MAX_VALUE - 1)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.next(), Integer.MAX_VALUE - 1);
      assertEquals(iterator.advance(Integer.MAX_VALUE - 3), Integer.MAX_VALUE - 3);
      assertEquals(iterator.next(), Integer.MAX_VALUE - 4);
    }
  }

  @Test
  public void testSequentialAdvanceCalls() {
    // Test multiple sequential advance calls
    List<Pairs.IntPair> docIdRanges = Arrays.asList(
        new Pairs.IntPair(10, 20),
        new Pairs.IntPair(30, 40)
    );
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(docIdRanges, false);
    try (BlockDocIdIterator iterator = sortedDocIdSet.iterator()) {
      assertEquals(iterator.advance(35), 35);
      assertEquals(iterator.advance(32), 32);
      assertEquals(iterator.advance(25), 20);
      assertEquals(iterator.advance(15), 15);
      assertEquals(iterator.advance(10), 10);
      assertEquals(iterator.advance(5), Constants.EOF);
    }
  }
}
