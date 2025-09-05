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

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.segment.spi.Constants;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/// Tests for MatchAllDocIdIterator
public class MatchAllDocIdIteratorTest {

  @Test
  public void testAscendingIteratorBasicNext() {
    // Test basic next() functionality for ascending iterator
    int numDocs = 5;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), 1);
    assertEquals(iterator.next(), 2);
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.next(), 4);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorBasicNext() {
    // Test basic next() functionality for descending iterator
    int numDocs = 5;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.next(), 4);
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.next(), 2);
    assertEquals(iterator.next(), 1);
    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorAdvance() {
    // Test advance() functionality for ascending iterator
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.advance(3), 3);
    assertEquals(iterator.next(), 4);
    assertEquals(iterator.advance(7), 7);
    assertEquals(iterator.next(), 8);
    assertEquals(iterator.advance(15), Constants.EOF); // Beyond numDocs
  }

  @Test
  public void testDescendingIteratorAdvance() {
    // Test advance() functionality for descending iterator
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.advance(7), 7);
    assertEquals(iterator.next(), 6);
    assertEquals(iterator.advance(3), 3);
    assertEquals(iterator.next(), 2);
    assertEquals(iterator.advance(0), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDescendingIteratorAdvanceWithInvalidTarget() {
    // Test that descending iterator throws exception when advancing to invalid target
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    // First call should work
    assertEquals(iterator.advance(7), 7);

    // This should throw IllegalArgumentException because 8 > 7
    iterator.advance(8);
  }

  @Test
  public void testAscendingIteratorWithZeroDocs() {
    // Test ascending iterator with zero documents
    int numDocs = 0;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.next(), Constants.EOF);
    assertEquals(iterator.advance(0), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorWithZeroDocs() {
    // Test descending iterator with zero documents
    int numDocs = 0;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorWithOneDocs() {
    // Test ascending iterator with one document
    int numDocs = 1;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorWithOneDoc() {
    // Test descending iterator with one document
    int numDocs = 1;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorAdvanceToCurrentPosition() {
    // Test advancing to current position in ascending iterator
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.advance(0), 0); // Should return 0 again
    assertEquals(iterator.next(), 1);
  }

  @Test
  public void testAscendingIteratorAdvanceBeyondEnd() {
    // Test advancing beyond the end in ascending iterator
    int numDocs = 5;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.advance(10), Constants.EOF);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorAdvanceToZero() {
    // Test advancing to zero in descending iterator
    int numDocs = 5;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.advance(0), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorExhaustionBehavior() {
    // Test behavior after ascending iterator is exhausted
    int numDocs = 3;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), 1);
    assertEquals(iterator.next(), 2);
    assertEquals(iterator.next(), Constants.EOF);

    // After exhaustion, should continue returning EOF
    assertEquals(iterator.next(), Constants.EOF);
    assertEquals(iterator.advance(5), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorExhaustionBehavior() {
    // Test behavior after descending iterator is exhausted
    int numDocs = 3;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.next(), 2);
    assertEquals(iterator.next(), 1);
    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), Constants.EOF);

    // After exhaustion, should continue returning EOF
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorMixedAdvanceAndNext() {
    // Test mixing advance() and next() calls in ascending iterator
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.advance(2), 2);
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.advance(5), 5);
    assertEquals(iterator.next(), 6);
    assertEquals(iterator.advance(8), 8);
    assertEquals(iterator.next(), 9);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorMixedAdvanceAndNext() {
    // Test mixing advance() and next() calls in descending iterator
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.advance(7), 7);
    assertEquals(iterator.next(), 6);
    assertEquals(iterator.advance(4), 4);
    assertEquals(iterator.next(), 3);
    assertEquals(iterator.advance(1), 1);
    assertEquals(iterator.next(), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorAdvanceSequence() {
    // Test multiple advance calls in sequence for ascending iterator
    int numDocs = 20;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.advance(5), 5);
    assertEquals(iterator.advance(10), 10);
    assertEquals(iterator.advance(15), 15);
    assertEquals(iterator.advance(25), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorAdvanceSequence() {
    // Test multiple advance calls in sequence for descending iterator
    int numDocs = 20;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.advance(15), 15);
    assertEquals(iterator.advance(10), 10);
    assertEquals(iterator.advance(5), 5);
    assertEquals(iterator.advance(0), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorLargeNumDocs() {
    // Test ascending iterator with large number of documents
    int numDocs = 100000;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    assertEquals(iterator.next(), 0);
    assertEquals(iterator.advance(50000), 50000);
    assertEquals(iterator.next(), 50001);
    assertEquals(iterator.advance(99999), 99999);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testDescendingIteratorLargeNumDocs() {
    // Test descending iterator with large number of documents
    int numDocs = 100000;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.next(), 99999);
    assertEquals(iterator.advance(50000), 50000);
    assertEquals(iterator.next(), 49999);
    assertEquals(iterator.advance(0), 0);
    assertEquals(iterator.next(), Constants.EOF);
  }

  @Test
  public void testAscendingIteratorCompleteIteration() {
    // Test complete iteration through all documents in ascending order
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, true);

    List<Integer> result = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }

    List<Integer> expected = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    assertEquals(result, expected);
  }

  @Test
  public void testDescendingIteratorCompleteIteration() {
    // Test complete iteration through all documents in descending order
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    List<Integer> result = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }

    List<Integer> expected = List.of(9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
    assertEquals(result, expected);
  }

  @Test
  public void testDescendingIteratorAdvanceMonotonicity() {
    // Test that descending iterator maintains monotonicity constraint
    int numDocs = 10;
    MatchAllDocIdIterator iterator = MatchAllDocIdIterator.create(numDocs, false);

    assertEquals(iterator.advance(8), 8);
    assertEquals(iterator.advance(5), 5);
    assertEquals(iterator.advance(2), 2);

    // Each advance should work with decreasing targets
    try {
      iterator.advance(7); // This should fail since 7 > 2
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }
}