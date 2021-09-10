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

package org.apache.pinot.segment.local.utils.nativefst;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.listEqualsIgnoreOrder;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.regexQueryNrHits;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.regexQueryNrHitsWithResults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Stress test -- load 51 million words (1.5 million unique words) and perform tests
 */
public class FSTLargeStressTest {
  private static FST _nativeFST;
  private static org.apache.lucene.util.fst.FST _fst;

  @BeforeClass
  public static void setUp()
      throws Exception {
    SortedMap<String, Integer> inputStrings = new TreeMap<>();
    InputStream fileInputStream;
    InputStreamReader inputStreamReader;
    BufferedReader bufferedReader;

    File file = new File("./src/test/resources/data/largewords.txt");

    fileInputStream = new FileInputStream(file);
    inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
    bufferedReader = new BufferedReader(inputStreamReader);

    String currentWord;
    int i = 0;
    while ((currentWord = bufferedReader.readLine()) != null) {
      inputStrings.put(currentWord, i);
      i++;
    }

    _nativeFST = FSTBuilder.buildFST(inputStrings);
    _fst = org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST(inputStrings);
  }

  @Test
  public void testRegex1()
      throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("q.[aeiou]c.*", _fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("q.[aeiou]c.*", _nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex2()
      throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ba.*", _fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ba.*", _nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex3()
      throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("b.*", _fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("b.*", _nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex5()
      throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*a", _fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*a", _nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRandomWords()
      throws IOException {
    assertEquals(1, regexQueryNrHits("respuestas", _nativeFST));
    assertEquals(1, regexQueryNrHits("Berge", _nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx198595", _nativeFST));
    assertEquals(1, regexQueryNrHits("popular", _nativeFST));
    assertEquals(1, regexQueryNrHits("Montella", _nativeFST));
    assertEquals(1, regexQueryNrHits("notably", _nativeFST));
    assertEquals(1, regexQueryNrHits("accepted", _nativeFST));
    assertEquals(1, regexQueryNrHits("challenging", _nativeFST));
    assertEquals(1, regexQueryNrHits("insurance", _nativeFST));
    assertEquals(1, regexQueryNrHits("Calls", _nativeFST));
    assertEquals(1, regexQueryNrHits("certified", _nativeFST));
    assertEquals(1, regexQueryNrHits(".*196169", _nativeFST));
    assertEquals(4290, regexQueryNrHits(".*wx.*", _nativeFST));
    assertEquals(1, regexQueryNrHits("keeps", _nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx160430", _nativeFST));
    assertEquals(1, regexQueryNrHits("called", _nativeFST));
    assertEquals(1, regexQueryNrHits("Rid", _nativeFST));
    assertEquals(1, regexQueryNrHits("Computer", _nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx871194", _nativeFST));
    assertEquals(1, regexQueryNrHits("control", _nativeFST));
    assertEquals(1, regexQueryNrHits("Gassy", _nativeFST));
    assertEquals(1, regexQueryNrHits("Nut", _nativeFST));
    assertEquals(1, regexQueryNrHits("Strangle", _nativeFST));
    assertEquals(1, regexQueryNrHits("ANYTHING", _nativeFST));
    assertEquals(1, regexQueryNrHits("RiverMusic", _nativeFST));
    assertEquals(1, regexQueryNrHits("\\@qwx420154", _nativeFST));
  }
}
