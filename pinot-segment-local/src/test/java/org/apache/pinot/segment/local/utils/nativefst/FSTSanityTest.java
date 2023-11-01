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
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.fst.RegexpMatcher;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTSerializerImpl;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Load a 0.5 million unique words data set and do the same set of queries on Lucene FST and
 * native FST and compare results
 */
public class FSTSanityTest {
  private FST _nativeFST;
  private org.apache.lucene.util.fst.FST _fst;

  @BeforeClass
  public void setUp()
      throws Exception {
    SortedMap<String, Integer> input = new TreeMap<>();
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("data/words.txt"))))) {
      String currentWord;
      int i = 0;
      while ((currentWord = bufferedReader.readLine()) != null) {
        input.put(currentWord, i++);
      }
    }

    FST fst = FSTBuilder.buildFST(input);
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();

    //TODO: Atri
    _nativeFST = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true, 0);
    _fst = org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST(input);
  }

  @Test
  public void testRegex()
      throws IOException {
    for (String regex : new String[]{"q.[aeiou]c.*", "a.*", "b.*", ".*", ".*tion", "abai.*", ".*ated", ".*ed",
        ".*pot.*", ".*a"}) {
      testRegex(regex);
    }
  }

  private void testRegex(String regex)
      throws IOException {
    List<Long> nativeResults = FSTTestUtils.regexQueryNrHitsWithResults(regex, _nativeFST);
    List<Long> results = RegexpMatcher.regexMatch(regex, _fst);
    nativeResults.sort(null);
    results.sort(null);
    assertEquals(nativeResults, results);
  }
}
