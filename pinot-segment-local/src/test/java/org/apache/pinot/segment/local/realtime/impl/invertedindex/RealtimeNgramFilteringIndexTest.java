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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class RealtimeNgramFilteringIndexTest {
  // ngram length from 2 to 3
  private RealtimeNgramFilteringIndex _ngram2To3Index;
  // ngram length from 2 to 2
  private RealtimeNgramFilteringIndex _ngram2To2Index;

  @BeforeClass
  public void setUp()
      throws Exception {
    _ngram2To3Index = new RealtimeNgramFilteringIndex("col", 2, 3);
    _ngram2To2Index = new RealtimeNgramFilteringIndex("col", 2, 2);
    List<String> documents = getTextData();

    for (String doc : documents) {
      _ngram2To3Index.add(doc);
      _ngram2To2Index.add(doc);
    }
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _ngram2To3Index.close();
    _ngram2To2Index.close();
  }

  @Test
  public void testQueries() {
    String ngramQuery = "drew";
    testSelectionResults(_ngram2To3Index, ngramQuery, Arrays.asList(0, 1));
    testSelectionResults(_ngram2To2Index, ngramQuery, Arrays.asList(0, 1, 4));

    ngramQuery = "rew";
    testSelectionResults(_ngram2To3Index, ngramQuery, Arrays.asList(0, 1, 4));
    testSelectionResults(_ngram2To2Index, ngramQuery, Arrays.asList(0, 1, 4, 5));

    ngramQuery = "re";
    testSelectionResults(_ngram2To3Index, ngramQuery, Arrays.asList(0, 1, 4, 5));
    testSelectionResults(_ngram2To2Index, ngramQuery, Arrays.asList(0, 1, 4, 5));

    ngramQuery = "r";
    testSelectionResults(_ngram2To3Index, ngramQuery, null);
    testSelectionResults(_ngram2To2Index, ngramQuery, null);
  }

  private List<String> getTextData() {
    return Arrays.asList("andrew", "drerew", "draw", "dr", "dr and rew", "ew re");
  }

  private void testSelectionResults(RealtimeNgramFilteringIndex realtimeNgramFilteringIndex, String nativeQuery, List<Integer> results) {
    ImmutableRoaringBitmap resultMap = realtimeNgramFilteringIndex.getDocIds(nativeQuery);
    if (resultMap == null) {
      assertNull(results);
      return;
    }

    assertEquals(resultMap.getCardinality(), results.size());
    for (int result : results) {
      assertTrue(resultMap.contains(result));
    }
  }
}
