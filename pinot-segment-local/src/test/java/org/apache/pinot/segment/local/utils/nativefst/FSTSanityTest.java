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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTSerializerImpl;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.listEqualsIgnoreOrder;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.regexQueryNrHitsWithResults;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Load a 0.5 million unique words data set and do the same set of queries on Lucene FST and
 * native FST and compare results
 */
public class FSTSanityTest {
  private FST nativeFST;
  private org.apache.lucene.util.fst.FST fst;

  @BeforeTest
  public void setUp() throws Exception {
    SortedMap<String, Integer> inputStrings = new TreeMap<>();
    InputStream fileInputStream = null;
    InputStreamReader inputStreamReader = null;
    BufferedReader bufferedReader = null;

    File file = new File("./src/test/resources/data/words.txt");

    fileInputStream = new FileInputStream(file);
    inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
    bufferedReader = new BufferedReader(inputStreamReader);

    String currentWord;
    int i = 0;
    while((currentWord = bufferedReader.readLine()) != null) {
      inputStrings.put(currentWord, i);
      i++;
    }

    FST FST = FSTBuilder.buildFST(inputStrings);
    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers()
            .serialize(FST, new ByteArrayOutputStream())
            .toByteArray();

    nativeFST = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);
    fst = org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST(inputStrings);
  }

  @Test
  public void testRegex1() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("q.[aeiou]c.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("q.[aeiou]c.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex2() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("a.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("a.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex3() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("b.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("b.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex4() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*", nativeFST);

    assertEquals(results.size(), nativeResults.size());
  }

  @Test
  public void testRegex5() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*landau", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*landau", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex6() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch("landau.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults("landau.*", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }


  @Test
  public void testRegex7() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ated", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ated", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex8() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*ed", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*ed", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex9() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*pot.*", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*pot.*", nativeFST);
    
    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
  }

  @Test
  public void testRegex10() throws IOException {
    List<Long> results = RegexpMatcher.regexMatch(".*a", fst);
    List<Long> nativeResults = regexQueryNrHitsWithResults(".*a", nativeFST);

    assertTrue(listEqualsIgnoreOrder(results, nativeResults));
    assertEquals(results.size(), nativeResults.size());
  }
}
