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
package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class MutableFSTSanityTest {
  private MutableFST _mutableFST;
  private org.apache.lucene.util.fst.FST _fst;

  @BeforeClass
  public void setUp()
      throws Exception {
    _mutableFST = new MutableFSTImpl();

    SortedMap<String, Integer> input = new TreeMap<>();
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("data/words.txt"))))) {
      String currentWord;
      int i = 0;
      while ((currentWord = bufferedReader.readLine()) != null) {
        _mutableFST.addPath(currentWord, i);
        input.put(currentWord, i++);
      }
    }

    _fst = org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST(input);
  }

  @Test
  public void testRegex()
      throws IOException {
    for (String regex : new String[]{"q.[aeiou]c.*", "a.*", "b.*", ".*", ".*landau", "landau.*", ".*ated", ".*ed",
        ".*pot.*", ".*a"}) {
      testRegex(regex);
    }
  }

  private void testRegex(String regex)
      throws IOException {
    List<Long> nativeResults = regexQueryNrHitsWithResults(regex, _mutableFST);
    List<Long> results = RegexpMatcher.regexMatch(regex, _fst);
    nativeResults.sort(null);
    results.sort(null);
    assertEquals(nativeResults, results);
  }

  /**
   * Return all matches for given regex
   */
  public static List<Long> regexQueryNrHitsWithResults(String regex, MutableFST fst) {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    org.apache.pinot.segment.local.utils.nativefst.utils.RealTimeRegexpMatcher.regexMatch(regex, fst, writer::add);
    MutableRoaringBitmap resultBitMap = writer.get();
    List<Long> resultList = new ArrayList<>();

    for (int dictId : resultBitMap) {
      resultList.add((long) dictId);
    }

    return resultList;
  }
}
