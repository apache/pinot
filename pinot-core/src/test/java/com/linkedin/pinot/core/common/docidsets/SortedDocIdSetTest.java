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
package com.linkedin.pinot.core.common.docidsets;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;


public class SortedDocIdSetTest {
  @Test
  public void testEmpty() {
    List<Pair<Integer, Integer>> pairs = new ArrayList<Pair<Integer, Integer>>();
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertTrue("Expected empty result set but got:" + result, result.isEmpty());
  }

  @Test
  public void testOnePair() {
    List<Pair<Integer, Integer>> pairs = new ArrayList<Pair<Integer, Integer>>();
    pairs.add(Pair.of(0, 9));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    System.out.println(result);
    Assert.assertEquals(10, result.size());
  }

  @Test
  public void testTwoPair() {
    List<Pair<Integer, Integer>> pairs = new ArrayList<Pair<Integer, Integer>>();
    pairs.add(Pair.of(90, 99));
    pairs.add(Pair.of(100, 109));

    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet(pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    System.out.println(result);
    Assert.assertEquals(20, result.size());
  }
}
