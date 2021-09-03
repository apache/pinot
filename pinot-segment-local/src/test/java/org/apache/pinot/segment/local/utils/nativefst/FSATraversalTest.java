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

import static java.nio.charset.StandardCharsets.*;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.AUTOMATON_HAS_PREFIX;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.EXACT_MATCH;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.NO_MATCH;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.SEQUENCE_IS_A_PREFIX;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.convertToBytes;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.regexQueryNrHits;
import static org.apache.pinot.segment.local.utils.nativefst.FSATestUtils.suffixes;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSA5Serializer;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSABuilder;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


/**
 * Tests {@link FSATraversal}.
 *
 * This class also holds tests for {@link RegexpMatcher} since they both perform FSA traversals
 */
public final class FSATraversalTest {
  private FSA fsa;
  private FSA regexFSA;

  @BeforeTest
  public void setUp() throws Exception {
    File file = new File("./src/test/resources/data/en_tst.dict");
    fsa = FSA.read(new FileInputStream(file), false,
        new DirectMemoryManager(FSATraversalTest.class.getName()));

    String regexTestInputString = "the quick brown fox jumps over the lazy ??? dog dddddd 493432 49344 [foo] 12.3 uick \\foo\\";
    String[] splitArray = regexTestInputString.split("\\s+");
    byte[][] bytesArray = convertToBytes(splitArray);

    Arrays.sort(bytesArray, FSABuilder.LEXICAL_ORDERING);

    FSABuilder fsaBuilder = new FSABuilder();

    for (byte[] currentArray : bytesArray) {
      fsaBuilder.add(currentArray, 0, currentArray.length, -1);
    }

    regexFSA = fsaBuilder.complete();
  }

  @Test
  public void testAutomatonHasPrefixBug() throws Exception {
    FSA fsa = FSABuilder.build(Arrays.asList(
        "a".getBytes(UTF_8),
        "ab".getBytes(UTF_8),
        "abc".getBytes(UTF_8),
        "ad".getBytes(UTF_8),
        "bcd".getBytes(UTF_8),
        "bce".getBytes(UTF_8)), new int[] {10, 11, 12, 13, 14, 15});

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(fsa, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa5 = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    FSATraversal fsaTraversal = new FSATraversal(fsa5);
    assertEquals(EXACT_MATCH, fsaTraversal.match("a".getBytes(UTF_8)).kind);
    assertEquals(EXACT_MATCH, fsaTraversal.match("ab".getBytes(UTF_8)).kind);
    assertEquals(EXACT_MATCH, fsaTraversal.match("abc".getBytes(UTF_8)).kind);
    assertEquals(EXACT_MATCH, fsaTraversal.match("ad".getBytes(UTF_8)).kind);

    assertEquals(SEQUENCE_IS_A_PREFIX, fsaTraversal.match("b".getBytes(UTF_8)).kind);
    assertEquals(SEQUENCE_IS_A_PREFIX, fsaTraversal.match("bc".getBytes(UTF_8)).kind);

    MatchResult m;

    m = fsaTraversal.match("abcd".getBytes(UTF_8));
    assertEquals(AUTOMATON_HAS_PREFIX, m.kind);
    assertEquals(3, m.index);

    m = fsaTraversal.match("ade".getBytes(UTF_8));
    assertEquals(AUTOMATON_HAS_PREFIX, m.kind);
    assertEquals(2, m.index);

    m = fsaTraversal.match("ax".getBytes(UTF_8));
    assertEquals(AUTOMATON_HAS_PREFIX, m.kind);
    assertEquals(1, m.index);

    assertEquals(NO_MATCH, fsaTraversal.match("d".getBytes(UTF_8)).kind);
  }

  @Test
  public void testTraversalWithIterable() {
    int count = 0;
    for (ByteBuffer bb : fsa.getSequences()) {
      assertEquals(0, bb.arrayOffset());
      assertEquals(0, bb.position());
      count++;
    }
    assertEquals(346773, count);
  }

  @Test
  public void testRecursiveTraversal() {
    final int[] counter = new int[] { 0 };

    class Recursion {
      public void dumpNode(final int node) {
        int arc = fsa.getFirstArc(node);
        do {
          if (fsa.isArcFinal(arc)) {
            counter[0]++;
          }

          if (!fsa.isArcTerminal(arc)) {
            dumpNode(fsa.getEndNode(arc));
          }

          arc = fsa.getNextArc(arc);
        } while (arc != 0);
      }
    }

    new Recursion().dumpNode(fsa.getRootNode());

    assertEquals(346773, counter[0]);
  }

  @Test
  public void testMatch() throws IOException {
    File file = new File("./src/test/resources/data/abc.fsa");
    final FSA fsa = FSA.read(new FileInputStream(file), false,
        new DirectMemoryManager(FSATraversalTest.class.getName()));
    final FSATraversal traversalHelper = new FSATraversal(fsa);

    MatchResult m = traversalHelper.match("ax".getBytes());
    assertEquals(AUTOMATON_HAS_PREFIX, m.kind);
    assertEquals(1, m.index);
    assertEquals(new HashSet<String>(Arrays.asList("ba", "c")), suffixes(fsa, m.node));

    assertEquals(EXACT_MATCH, traversalHelper.match("aba".getBytes()).kind);

    m = traversalHelper.match("abalonger".getBytes());
    assertEquals(AUTOMATON_HAS_PREFIX, m.kind);
    assertEquals("longer", "abalonger".substring(m.index));

    m = traversalHelper.match("ab".getBytes());
    assertEquals(SEQUENCE_IS_A_PREFIX, m.kind);
    assertEquals(new HashSet<String>(Arrays.asList("a")), suffixes(fsa, m.node));
  }

  @Test
  public void testRegexMatcherPrefix() throws IOException {
    String firstString = "he";
    String secondString = "hp";
    FSABuilder builder = new FSABuilder();

    builder.add(firstString.getBytes(UTF_8), 0, firstString.length(), 127);
    builder.add(secondString.getBytes(UTF_8), 0, secondString.length(), 136);

    FSA s = builder.complete();

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    List<Long> results = RegexpMatcher.regexMatch("h.*", fsa);

    assertEquals(2,  results.size());
  }

  @Test
  public void testRegexMatcherSuffix() throws IOException {
    String firstString = "aeh";
    String secondString = "pfh";
    FSABuilder builder = new FSABuilder();

    builder.add(firstString.getBytes(UTF_8), 0, firstString.length(), 127);
    builder.add(secondString.getBytes(UTF_8), 0, secondString.length(), 136);

    FSA s = builder.complete();

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    List<Long> results = RegexpMatcher.regexMatch(".*h", fsa);

    assertEquals(2,  results.size());
  }

  @Test
  public void testRegexMatcherSuffix2() throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello-world", 12);
    x.put("hello-world123", 21);
    x.put("still", 123);

    FSA s = FSABuilder.buildFSA(x);

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    List<Long> results = RegexpMatcher.regexMatch(".*123", fsa);

    assertEquals(1, results.size());

    results = RegexpMatcher.regexMatch(".till", fsa);

    assertEquals(1, results.size());
  }

  @Test
  public void testRegexMatcherMatchAny() throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello-world", 12);
    x.put("hello-world123", 21);
    x.put("still", 123);

    FSA s = FSABuilder.buildFSA(x);

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    List<Long> results = RegexpMatcher.regexMatch("hello.*123", fsa);

    assertEquals(results.size(), 1);

    assertTrue(results.get(0) == 21);

    results = RegexpMatcher.regexMatch("hello.*", fsa);

    assertEquals(results.size(), 2);
  }

  @Test
  public void testFSAToString() throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello", 12);
    x.put("help", 21);
    x.put("helipad", 123);
    x.put("hot", 123);

    FSA s = FSABuilder.buildFSA(x);

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    String fsaString = FSA5.printToString(fsa);

    assert fsaString != null;
  }

  @Test
  public void testRegexMatcherMatchQuestionMark() throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("car", 12);
    x.put("cars", 21);

    FSA s = FSABuilder.buildFSA(x);

    final byte[] fsaData =
        new FSA5Serializer().withNumbers()
            .serialize(s, new ByteArrayOutputStream())
            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class, true);

    List<Long> results = RegexpMatcher.regexMatch("cars?", fsa);

    assertEquals(results.size(), 2);
  }

  @Test
  public void testRegex1() {
    assertEquals(1, regexQueryNrHits("q.[aeiou]c.*", regexFSA));
  }

  @Test
  public void testRegex2() {
    assertEquals(1, regexQueryNrHits(".[aeiou]c.*", regexFSA));
    assertEquals(1, regexQueryNrHits("q.[aeiou]c.", regexFSA));
  }

  @Test
  public void testCharacterClasses() {
    assertEquals(1, regexQueryNrHits("\\d*", regexFSA));
    assertEquals(1, regexQueryNrHits("\\d{6}", regexFSA));
    assertEquals(1, regexQueryNrHits("[a\\d]{6}", regexFSA));
    assertEquals(1, regexQueryNrHits("\\d{2,7}", regexFSA));
    assertEquals(0, regexQueryNrHits("\\d{4}", regexFSA));
    assertEquals(1, regexQueryNrHits("\\dog", regexFSA));
  }

  @Test
  public void testRegexComplement() {
    assertEquals(2, regexQueryNrHits("4934~[3]", regexFSA));
    // not the empty lang, i.e. match all docs
    assertEquals(16, regexQueryNrHits("~#", regexFSA));
  }

  /**
   * Test a corner case for backtracking: In this case the term dictionary has 493432 followed by
   * 49344. When backtracking from 49343... to 4934, it's necessary to test that 4934 itself is ok
   * before trying to append more characters.
   */
  @Test
  public void testBacktracking() {
    assertEquals(1, regexQueryNrHits("4934[314]", regexFSA));
  }
}
