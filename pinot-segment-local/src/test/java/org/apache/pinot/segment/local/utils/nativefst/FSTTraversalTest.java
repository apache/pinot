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

import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTSerializerImpl;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.convertToBytes;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.regexQueryNrHits;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.suffixes;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.AUTOMATON_HAS_PREFIX;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.EXACT_MATCH;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.NO_MATCH;
import static org.apache.pinot.segment.local.utils.nativefst.MatchResult.SEQUENCE_IS_A_PREFIX;
import static org.testng.Assert.assertEquals;


/**
 * Tests {@link FSTTraversal}.
 *
 * This class also holds tests for {@link RegexpMatcher} since they both perform FST traversals
 */
public class FSTTraversalTest {
  private FST _fst;
  private FST _regexFST;

  @BeforeClass
  public void setUp()
      throws Exception {
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("data/en_tst.dict")) {
      _fst = FST.read(inputStream, false, new DirectMemoryManager(FSTTraversalTest.class.getName()));
    }

    String regexTestInputString =
        "the quick brown fox jumps over the lazy ???" + "dog dddddd 493432 49344 [foo] 12.3 uick \\foo\\";
    String[] splitArray = regexTestInputString.split("\\s+");
    byte[][] bytesArray = convertToBytes(splitArray);
    Arrays.sort(bytesArray, FSTBuilder.LEXICAL_ORDERING);

    FSTBuilder fstBuilder = new FSTBuilder();
    for (byte[] currentArray : bytesArray) {
      fstBuilder.add(currentArray, 0, currentArray.length, -1);
    }

    _regexFST = fstBuilder.complete();
  }

  @Test
  public void testAutomatonHasPrefixBug()
      throws Exception {
    FST fst = FSTBuilder.build(
        Arrays.asList("a".getBytes(UTF_8), "ab".getBytes(UTF_8), "abc".getBytes(UTF_8), "ad".getBytes(UTF_8),
            "bcd".getBytes(UTF_8), "bce".getBytes(UTF_8)), new int[]{10, 11, 12, 13, 14, 15});
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    FSTTraversal fstTraversal = new FSTTraversal(fst);
    assertEquals(fstTraversal.match("a".getBytes(UTF_8))._kind, EXACT_MATCH);
    assertEquals(fstTraversal.match("ab".getBytes(UTF_8))._kind, EXACT_MATCH);
    assertEquals(fstTraversal.match("abc".getBytes(UTF_8))._kind, EXACT_MATCH);
    assertEquals(fstTraversal.match("ad".getBytes(UTF_8))._kind, EXACT_MATCH);

    assertEquals(fstTraversal.match("b".getBytes(UTF_8))._kind, SEQUENCE_IS_A_PREFIX);
    assertEquals(fstTraversal.match("bc".getBytes(UTF_8))._kind, SEQUENCE_IS_A_PREFIX);

    MatchResult m = fstTraversal.match("abcd".getBytes(UTF_8));
    assertEquals(m._kind, AUTOMATON_HAS_PREFIX);
    assertEquals(m._index, 3);

    m = fstTraversal.match("ade".getBytes(UTF_8));
    assertEquals(m._kind, AUTOMATON_HAS_PREFIX);
    assertEquals(m._index, 2);

    m = fstTraversal.match("ax".getBytes(UTF_8));
    assertEquals(m._kind, AUTOMATON_HAS_PREFIX);
    assertEquals(m._index, 1);

    assertEquals(fstTraversal.match("d".getBytes(UTF_8))._kind, NO_MATCH);
  }

  @Test
  public void testTraversalWithIterable() {
    int count = 0;
    for (ByteBuffer bb : _fst.getSequences()) {
      assertEquals(bb.arrayOffset(), 0);
      assertEquals(bb.position(), 0);
      count++;
    }
    assertEquals(count, 346773);
  }

  @Test
  public void testRecursiveTraversal() {
    int[] counter = new int[]{0};

    class Recursion {
      public void dumpNode(int node) {
        int arc = _fst.getFirstArc(node);
        do {
          if (_fst.isArcFinal(arc)) {
            counter[0]++;
          }

          if (!_fst.isArcTerminal(arc)) {
            dumpNode(_fst.getEndNode(arc));
          }

          arc = _fst.getNextArc(arc);
        } while (arc != 0);
      }
    }

    new Recursion().dumpNode(_fst.getRootNode());

    assertEquals(counter[0], 346773);
  }

  @Test
  public void testMatch()
      throws IOException {
    File file = new File("./src/test/resources/data/abc.native.fst");
    FST fst = FST.read(new FileInputStream(file), false, new DirectMemoryManager(FSTTraversalTest.class.getName()));
    FSTTraversal traversalHelper = new FSTTraversal(fst);

    MatchResult m = traversalHelper.match("ax".getBytes());
    assertEquals(m._kind, AUTOMATON_HAS_PREFIX);
    assertEquals(m._index, 1);
    assertEquals(suffixes(fst, m._node), Sets.newHashSet("ba", "c"));

    assertEquals(traversalHelper.match("aba".getBytes())._kind, EXACT_MATCH);

    m = traversalHelper.match("abalonger".getBytes());
    assertEquals(m._kind, AUTOMATON_HAS_PREFIX);
    assertEquals("abalonger".substring(m._index), "longer");

    m = traversalHelper.match("ab".getBytes());
    assertEquals(m._kind, SEQUENCE_IS_A_PREFIX);
    assertEquals(suffixes(fst, m._node), Sets.newHashSet("a"));
  }

  @Test
  public void testRegexMatcherPrefix()
      throws IOException {
    String firstString = "he";
    String secondString = "hp";
    FSTBuilder builder = new FSTBuilder();

    builder.add(firstString.getBytes(UTF_8), 0, firstString.length(), 127);
    builder.add(secondString.getBytes(UTF_8), 0, secondString.length(), 136);

    FST fst = builder.complete();
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch("h.*", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegexMatcherSuffix()
      throws IOException {
    String firstString = "aeh";
    String secondString = "pfh";
    FSTBuilder builder = new FSTBuilder();

    builder.add(firstString.getBytes(UTF_8), 0, firstString.length(), 127);
    builder.add(secondString.getBytes(UTF_8), 0, secondString.length(), 136);

    FST fst = builder.complete();
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch(".*h", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegexMatcherSuffix2()
      throws IOException {
    SortedMap<String, Integer> input = new TreeMap<>();
    input.put("hello-world", 12);
    input.put("hello-world123", 21);
    input.put("still", 123);

    FST fst = FSTBuilder.buildFST(input);
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch(".*123", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 1);

    writer.reset();

    RegexpMatcher.regexMatch(".till", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 1);
  }

  @Test
  public void testRegexMatcherMatchAny()
      throws IOException {
    SortedMap<String, Integer> input = new TreeMap<>();
    input.put("hello-world", 12);
    input.put("hello-world123", 21);
    input.put("still", 123);

    FST fst = FSTBuilder.buildFST(input);
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch("hello.*123", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 1);

    writer.reset();
    RegexpMatcher.regexMatch("hello.*", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testFSTToString()
      throws IOException {
    SortedMap<String, Integer> input = new TreeMap<>();
    input.put("hello", 12);
    input.put("help", 21);
    input.put("helipad", 123);
    input.put("hot", 123);

    FST fst = FSTBuilder.buildFST(input);
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    ImmutableFST.printToString(fst);
  }

  @Test
  public void testRegexMatcherMatchQuestionMark()
      throws IOException {
    SortedMap<String, Integer> input = new TreeMap<>();
    input.put("car", 12);
    input.put("cars", 21);

    FST fst = FSTBuilder.buildFST(input);
    byte[] fstData = new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    fst = FST.read(new ByteArrayInputStream(fstData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch("cars?", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegex1() {
    assertEquals(regexQueryNrHits("q.[aeiou]c.*", _regexFST), 1);
  }

  @Test
  public void testRegex2() {
    assertEquals(regexQueryNrHits(".[aeiou]c.*", _regexFST), 1);
    assertEquals(regexQueryNrHits("q.[aeiou]c.", _regexFST), 1);
  }

  @Test
  public void testCharacterClasses() {
    assertEquals(regexQueryNrHits("\\d*", _regexFST), 1);
    assertEquals(regexQueryNrHits("\\d{6}", _regexFST), 1);
    assertEquals(regexQueryNrHits("[a\\d]{6}", _regexFST), 1);
    assertEquals(regexQueryNrHits("\\d{2,7}", _regexFST), 1);
    assertEquals(regexQueryNrHits("\\d{4}", _regexFST), 0);
  }

  /**
   * Test a corner case for backtracking: In this case the term dictionary has 493432 followed by
   * 49344. When backtracking from 49343... to 4934, it's necessary to test that 4934 itself is ok
   * before trying to append more characters.
   */
  @Test
  public void testBacktracking() {
    assertEquals(regexQueryNrHits("4934[314]", _regexFST), 1);
  }
}
