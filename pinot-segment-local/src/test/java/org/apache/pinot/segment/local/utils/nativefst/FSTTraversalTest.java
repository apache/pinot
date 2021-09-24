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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTSerializerImpl;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.BeforeTest;
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
public final class FSTTraversalTest {
  private FST _fst;
  private FST _regexFST;

  @BeforeTest
  public void setUp()
      throws Exception {
    File file = new File("./src/test/resources/data/en_tst.dict");
    _fst = FST.read(new FileInputStream(file), false, new DirectMemoryManager(FSTTraversalTest.class.getName()));

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
    FST fst = FSTBuilder.build(Arrays
        .asList("a".getBytes(UTF_8), "ab".getBytes(UTF_8), "abc".getBytes(UTF_8), "ad".getBytes(UTF_8),
            "bcd".getBytes(UTF_8), "bce".getBytes(UTF_8)), new int[]{10, 11, 12, 13, 14, 15});

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST immutableFST = org.apache.pinot.segment.local.utils.nativefst.FST
        .read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

    FSTTraversal fstTraversal = new FSTTraversal(immutableFST);
    assertEquals(EXACT_MATCH, fstTraversal.match("a".getBytes(UTF_8))._kind);
    assertEquals(EXACT_MATCH, fstTraversal.match("ab".getBytes(UTF_8))._kind);
    assertEquals(EXACT_MATCH, fstTraversal.match("abc".getBytes(UTF_8))._kind);
    assertEquals(EXACT_MATCH, fstTraversal.match("ad".getBytes(UTF_8))._kind);

    assertEquals(SEQUENCE_IS_A_PREFIX, fstTraversal.match("b".getBytes(UTF_8))._kind);
    assertEquals(SEQUENCE_IS_A_PREFIX, fstTraversal.match("bc".getBytes(UTF_8))._kind);

    MatchResult m;

    m = fstTraversal.match("abcd".getBytes(UTF_8));
    assertEquals(AUTOMATON_HAS_PREFIX, m._kind);
    assertEquals(3, m._index);

    m = fstTraversal.match("ade".getBytes(UTF_8));
    assertEquals(AUTOMATON_HAS_PREFIX, m._kind);
    assertEquals(2, m._index);

    m = fstTraversal.match("ax".getBytes(UTF_8));
    assertEquals(AUTOMATON_HAS_PREFIX, m._kind);
    assertEquals(1, m._index);

    assertEquals(NO_MATCH, fstTraversal.match("d".getBytes(UTF_8))._kind);
  }

  @Test
  public void testTraversalWithIterable() {
    int count = 0;
    for (ByteBuffer bb : _fst.getSequences()) {
      assertEquals(0, bb.arrayOffset());
      assertEquals(0, bb.position());
      count++;
    }
    assertEquals(346773, count);
  }

  @Test
  public void testRecursiveTraversal() {
    final int[] counter = new int[]{0};

    class Recursion {
      public void dumpNode(final int node) {
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

    assertEquals(346773, counter[0]);
  }

  @Test
  public void testMatch()
      throws IOException {
    File file = new File("./src/test/resources/data/abc.fsa");
    final FST fst = org.apache.pinot.segment.local.utils.nativefst.FST
        .read(new FileInputStream(file), false, new DirectMemoryManager(FSTTraversalTest.class.getName()));
    final FSTTraversal traversalHelper = new FSTTraversal(fst);

    MatchResult m = traversalHelper.match("ax".getBytes());
    assertEquals(AUTOMATON_HAS_PREFIX, m._kind);
    assertEquals(1, m._index);
    assertEquals(new HashSet<String>(Arrays.asList("ba", "c")), suffixes(fst, m._node));

    assertEquals(EXACT_MATCH, traversalHelper.match("aba".getBytes())._kind);

    m = traversalHelper.match("abalonger".getBytes());
    assertEquals(AUTOMATON_HAS_PREFIX, m._kind);
    assertEquals("longer", "abalonger".substring(m._index));

    m = traversalHelper.match("ab".getBytes());
    assertEquals(SEQUENCE_IS_A_PREFIX, m._kind);
    assertEquals(new HashSet<String>(Arrays.asList("a")), suffixes(fst, m._node));
  }

  @Test
  public void testRegexMatcherPrefix()
      throws IOException {
    String firstString = "he";
    String secondString = "hp";
    FSTBuilder builder = new FSTBuilder();

    builder.add(firstString.getBytes(UTF_8), 0, firstString.length(), 127);
    builder.add(secondString.getBytes(UTF_8), 0, secondString.length(), 136);

    FST s = builder.complete();

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch("h.*", fst, writer::add);

    assertEquals(2, writer.get().getCardinality());
  }

  @Test
  public void testRegexMatcherSuffix()
      throws IOException {
    String firstString = "aeh";
    String secondString = "pfh";
    FSTBuilder builder = new FSTBuilder();

    builder.add(firstString.getBytes(UTF_8), 0, firstString.length(), 127);
    builder.add(secondString.getBytes(UTF_8), 0, secondString.length(), 136);

    FST s = builder.complete();

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch(".*h", fst, writer::add);

    assertEquals(2, writer.get().getCardinality());
  }

  @Test
  public void testRegexMatcherSuffix2()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello-world", 12);
    x.put("hello-world123", 21);
    x.put("still", 123);

    FST s = FSTBuilder.buildFST(x);

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch(".*123", fst, writer::add);

    assertEquals(1, writer.get().getCardinality());

    writer.reset();

    RegexpMatcher.regexMatch(".till", fst, writer::add);

    assertEquals(1, writer.get().getCardinality());
  }

  @Test
  public void testRegexMatcherMatchAny()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello-world", 12);
    x.put("hello-world123", 21);
    x.put("still", 123);

    FST s = FSTBuilder.buildFST(x);

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

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
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("hello", 12);
    x.put("help", 21);
    x.put("helipad", 123);
    x.put("hot", 123);

    FST s = FSTBuilder.buildFST(x);

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

    String fsaString = ImmutableFST.printToString(fst);

    assert fsaString != null;
  }

  @Test
  public void testRegexMatcherMatchQuestionMark()
      throws IOException {
    SortedMap<String, Integer> x = new TreeMap<>();
    x.put("car", 12);
    x.put("cars", 21);

    FST s = FSTBuilder.buildFST(x);

    System.out.println(s.toString());

    final byte[] fsaData =
        new FSTSerializerImpl().withNumbers().serialize(s, new ByteArrayOutputStream()).toByteArray();

    final ImmutableFST fst = FST.read(new ByteArrayInputStream(fsaData), ImmutableFST.class, true);

    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch("cars?", fst, writer::add);

    assertEquals(writer.get().getCardinality(), 2);
  }

  @Test
  public void testRegex1() {
    assertEquals(1, regexQueryNrHits("q.[aeiou]c.*", _regexFST));
  }

  @Test
  public void testRegex2() {
    assertEquals(1, regexQueryNrHits(".[aeiou]c.*", _regexFST));
    assertEquals(1, regexQueryNrHits("q.[aeiou]c.", _regexFST));
  }

  @Test
  public void testCharacterClasses() {
    assertEquals(1, regexQueryNrHits("\\d*", _regexFST));
    assertEquals(1, regexQueryNrHits("\\d{6}", _regexFST));
    assertEquals(1, regexQueryNrHits("[a\\d]{6}", _regexFST));
    assertEquals(1, regexQueryNrHits("\\d{2,7}", _regexFST));
    assertEquals(0, regexQueryNrHits("\\d{4}", _regexFST));
  }

  /**
   * Test a corner case for backtracking: In this case the term dictionary has 493432 followed by
   * 49344. When backtracking from 49343... to 4934, it's necessary to test that 4934 itself is ok
   * before trying to append more characters.
   */
  @Test
  public void testBacktracking() {
    assertEquals(1, regexQueryNrHits("4934[314]", _regexFST));
  }
}
