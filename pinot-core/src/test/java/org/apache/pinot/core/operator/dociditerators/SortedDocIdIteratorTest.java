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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.core.common.BlockDocIdIterator;
import org.apache.pinot.core.operator.docidsets.SortedDocIdSet;
import org.apache.pinot.segment.spi.Constants;
import org.apache.pinot.spi.utils.Pairs;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class SortedDocIdIteratorTest {

  @Test
  public void testPairWithSameStartAndEnd() {
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(Collections.singletonList(new Pairs.IntPair(1, 1)));
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    assertEquals(result, Collections.singletonList(1));
  }

  @Test
  public void testOneDocIdRange() {
    List<Pairs.IntPair> docIdRanges = Collections.singletonList(new Pairs.IntPair(5, 15));
    SortedDocIdIterator docIdIterator = new SortedDocIdSet(docIdRanges).iterator();
    assertEquals(docIdIterator.next(), 5);
    assertEquals(docIdIterator.next(), 6);
    assertEquals(docIdIterator.advance(8), 8);
    assertEquals(docIdIterator.advance(10), 10);
    assertEquals(docIdIterator.next(), 11);
    assertEquals(docIdIterator.advance(15), 15);
    assertEquals(docIdIterator.advance(16), Constants.EOF);
  }

  @Test
  public void testTwoDocIdRanges() {
    List<Pairs.IntPair> docIdRanges = Arrays.asList(new Pairs.IntPair(20, 25), new Pairs.IntPair(30, 35));
    SortedDocIdIterator docIdIterator = new SortedDocIdSet(docIdRanges).iterator();
    assertEquals(docIdIterator.advance(15), 20);
    assertEquals(docIdIterator.next(), 21);
    assertEquals(docIdIterator.next(), 22);
    assertEquals(docIdIterator.advance(27), 30);
    assertEquals(docIdIterator.next(), 31);
    assertEquals(docIdIterator.advance(35), 35);
    assertEquals(docIdIterator.next(), Constants.EOF);
  }

  @Test
  public void testDocIdRangesWithSingleDocument() {
    List<Pairs.IntPair> docIdRanges = Arrays
        .asList(new Pairs.IntPair(3, 3), new Pairs.IntPair(8, 8), new Pairs.IntPair(15, 15), new Pairs.IntPair(20, 20));
    SortedDocIdIterator docIdIterator = new SortedDocIdSet(docIdRanges).iterator();
    assertEquals(docIdIterator.next(), 3);
    assertEquals(docIdIterator.advance(5), 8);
    assertEquals(docIdIterator.next(), 15);
    assertEquals(docIdIterator.next(), 20);
    assertEquals(docIdIterator.advance(22), Constants.EOF);
  }
}
