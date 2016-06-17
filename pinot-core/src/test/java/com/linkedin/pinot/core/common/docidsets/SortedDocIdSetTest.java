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
package com.linkedin.pinot.core.common.docidsets;

import com.linkedin.pinot.common.utils.Pairs;
import com.linkedin.pinot.common.utils.Pairs.IntPair;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.docidsets.SortedDocIdSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SortedDocIdSetTest {
  @Test
  public void testEmpty() {
    List<IntPair> pairs = new ArrayList<IntPair>();
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet("Datasource-testCol", pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertTrue(result.isEmpty(), "Expected empty result set but got:" + result);
  }

  @Test
  public void testPairWithSameStartAndEnd() {
    List<IntPair> pairs = new ArrayList<IntPair>();
    pairs.add(Pairs.intPair(1, 1));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet("Datasource-testCol",pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertEquals(1, result.size());
  }

  @Test
  public void testOnePair() {
    List<IntPair> pairs = new ArrayList<IntPair>();
    pairs.add(Pairs.intPair(0, 9));
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet("Datasource-testCol", pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertEquals(10, result.size());
  }

  @Test
  public void testTwoPair() {
    List<IntPair> pairs = new ArrayList<IntPair>();
    pairs.add(Pairs.intPair(90, 99));
    pairs.add(Pairs.intPair(100, 109));

    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet("Datasource-testCol",pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertEquals(20, result.size());
  }

  @Test
  public void testCustom() {
    String rangeString;
    rangeString = "(9933,10195)]";
    testCustomRange(rangeString);
    rangeString =
        "[(0,229), (230,499), (500,807), (808,1146), (1147,1456), (1457,1762), (1763,2069), (2070,2313), (2314,2600), (2601,2909), (2910,3221), (3222,3531), (3532,3849), (3850,4148), (4149,4371), (4372,4690), (4691,5017), (5018,5309), (5310,5588), (5589,5863), (5864,6171), (6172,6439), (6440,6718), (6719,7015), (7016,7328), (7329,7637), (7638,7830), (7831,8074), (8075,8366), (8367,8735)]";
    testCustomRange(rangeString);
    rangeString =
        "[(0,316), (317,721), (722,1036), (1037,1251), (1252,1541), (1542,1925), (1926,2211), (2212,2517), (2518,2840), (2841,3156), (3157,3399), (3400,3707), (3708,4077), (4078,4407), (4408,4707), (4708,5000), (5001,5301), (5302,5531), (5532,5822), (5823,6111), (6112,6420), (6421,6726), (6727,7037), (7038,7324), (7325,7563), (7564,7846), (7847,8142), (8143,8428), (8429,8734), (8735,9007), (9008,9291)]";
    testCustomRange(rangeString);
    rangeString =
        "[(0,316), (317,634), (635,970), (971,1297), (1298,1628), (1629,1856), (1857,2149), (2150,2443), (2444,2764), (2765,3094), (3095,3419), (3420,3727), (3728,3961), (3962,4263), (4264,4551), (4552,4834), (4835,5154), (5155,5452), (5453,5773), (5774,6034), (6035,6342), (6343,6672), (6673,7031), (7032,7299), (7300,7540), (7541,7841), (7842,8137), (8138,8473), (8474,8813), (8814,9125), (9126,9377)]";
    testCustomRange(rangeString);

    rangeString =
        "[(0,325), (326,658), (659,946), (947,1308), (1309,1658), (1659,1922), (1923,2276), (2277,2601), (2602,2888), (2889,3192), (3193,3499), (3500,3810), (3811,4058), (4059,4335), (4336,4636), (4637,4940), (4941,5226), (5227,5549), (5550,5864), (5865,6094), (6095,6390), (6391,6689), (6690,6957), (6958,7246), (7247,7532), (7533,7919), (7920,8183), (8184,8485), (8486,8819), (8820,9135)]";
    testCustomRange(rangeString);

  }

  private void testCustomRange(String rangeString) {
    String trim =
        rangeString.replace('[', ' ').replace(']', ' ').replace('(', ' ').replace(')', ' ').replaceAll("[\\s]+", "");
    String[] splits = trim.split(",");
    Assert.assertTrue(splits.length % 2 == 0);
    List<Integer> expectedList = new ArrayList<Integer>();
    List<IntPair> pairs = new ArrayList<IntPair>();

    for (int i = 0; i < splits.length; i += 2) {
      int start = Integer.parseInt(splits[i]);
      int end = Integer.parseInt(splits[i + 1]);
      for (int val = start; val <= end; val++) {
        expectedList.add(val);
      }
      pairs.add(Pairs.intPair(start, end));
    }
    SortedDocIdSet sortedDocIdSet = new SortedDocIdSet("Datasource-testCol", pairs);
    BlockDocIdIterator iterator = sortedDocIdSet.iterator();
    List<Integer> result = new ArrayList<Integer>();
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      result.add(docId);
    }
    Assert.assertEquals(result.size(), expectedList.size());
    Assert.assertEquals(result, expectedList);
  }
}
