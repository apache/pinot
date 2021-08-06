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
package org.apache.pinot.fsa;

import static java.nio.charset.StandardCharsets.*;
import static org.apache.pinot.fsa.MatchResult.AUTOMATON_HAS_PREFIX;
import static org.apache.pinot.fsa.MatchResult.EXACT_MATCH;
import static org.apache.pinot.fsa.MatchResult.NO_MATCH;
import static org.apache.pinot.fsa.MatchResult.SEQUENCE_IS_A_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.pinot.fsa.builders.FSA5Serializer;
import org.apache.pinot.fsa.builders.FSABuilder;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link FSATraversal}.
 */
public final class FSATraversalTest extends TestBase {
  private FSA fsa;

  @Before
  public void setUp() throws Exception {
    fsa = FSA.read(this.getClass().getResourceAsStream("/resources/en_tst.dict"));
  }

  @Test
  public void testAutomatonHasPrefixBug() throws Exception {
      FSA fsa = FSABuilder.build(Arrays.asList(
          "a".getBytes(UTF_8),
          "ab".getBytes(UTF_8),
          "abc".getBytes(UTF_8),
          "ad".getBytes(UTF_8),
          "bcd".getBytes(UTF_8),
          "bce".getBytes(UTF_8)));

      FSATraversal fsaTraversal = new FSATraversal(fsa);
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
  public void testPerfectHash() throws IOException {
    byte[][] input = new byte[][] { 
      { 'a' }, 
      { 'a', 'b', 'a' }, 
      { 'a', 'c' }, 
      { 'b' }, 
      { 'b', 'a' }, 
      { 'c' }, };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input);

    final byte[] fsaData = 
        new FSA5Serializer().withNumbers()
                            .serialize(s, new ByteArrayOutputStream())
                            .toByteArray();

    final FSA5 fsa = FSA.read(new ByteArrayInputStream(fsaData), FSA5.class);
    final FSATraversal traversal = new FSATraversal(fsa);

    int i = 0;
    for (byte[] seq : input) {
      assertEquals(new String(seq), i++, traversal.perfectHash(seq));
    }

    // Check if the total number of sequences is encoded at the root node.
    assertEquals(6, fsa.getRightLanguageCount(fsa.getRootNode()));

    // Check sub/super sequence scenarios.
    assertEquals(AUTOMATON_HAS_PREFIX, traversal.perfectHash("abax".getBytes(UTF_8)));
    assertEquals(AUTOMATON_HAS_PREFIX, traversal.perfectHash("abx".getBytes(UTF_8)));
    assertEquals(SEQUENCE_IS_A_PREFIX, traversal.perfectHash("ab".getBytes(UTF_8)));
    assertEquals(NO_MATCH, traversal.perfectHash("d".getBytes(UTF_8)));
    assertEquals(NO_MATCH, traversal.perfectHash(new byte[] { 0 }));

    assertTrue(AUTOMATON_HAS_PREFIX < 0);
    assertTrue(SEQUENCE_IS_A_PREFIX < 0);
    assertTrue(NO_MATCH < 0);
  }

  /**
     * 
     */
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
    final FSA fsa = FSA.read(this.getClass().getResourceAsStream("/resources/abc.fsa"));
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

  /**
   * Return all sequences reachable from a given node, as strings.
   */
  private HashSet<String> suffixes(FSA fsa, int node) {
    HashSet<String> result = new HashSet<String>();
    for (ByteBuffer bb : fsa.getSequences(node)) {
      result.add(new String(bb.array(), bb.position(), bb.remaining(), UTF_8));
    }
    return result;
  }
}
