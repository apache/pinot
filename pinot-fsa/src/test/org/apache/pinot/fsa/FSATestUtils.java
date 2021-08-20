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

import java.io.IOException;
import java.nio.ByteBuffer;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.fsa.builders.FSABuilder;
import org.apache.pinot.fsa.utils.RegexpMatcher;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


public class FSATestUtils {
  /*
   * Generate a sorted list of random sequences.
   */
  public static byte[][] generateRandom(int count, MinMax length, MinMax alphabet) {
    final byte[][] input = new byte[count][];
    final Random rnd = new Random(randomLong());
    for (int i = 0; i < count; i++) {
      input[i] = randomByteSequence(rnd, length, alphabet);
    }
    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    return input;
  }

  /**
   * Generate a random string.
   */
  private static byte[] randomByteSequence(Random rnd, MinMax length, MinMax alphabet) {
    byte[] bytes = new byte[length.min + rnd.nextInt(length.range())];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (alphabet.min + rnd.nextInt(alphabet.range()));
    }
    return bytes;
  }

  /*
   * Check if the DFSA is correct with respect to the given input.
   */
  public static void checkCorrect(byte[][] input, FSA fsa) {
    // (1) All input sequences are in the right language.
    HashSet<ByteBuffer> rl = new HashSet<ByteBuffer>();
    for (ByteBuffer bb : fsa) {
      rl.add(ByteBuffer.wrap(Arrays.copyOf(bb.array(), bb.remaining())));
    }

    HashSet<ByteBuffer> uniqueInput = new HashSet<ByteBuffer>();
    for (byte[] sequence : input) {
      uniqueInput.add(ByteBuffer.wrap(sequence));
    }

    for (ByteBuffer sequence : uniqueInput) {
      if (!rl.remove(sequence)) {
        fail("Not present in the right language: " + SerializerTestBase.toString(sequence));
      }
    }

    // (2) No other sequence _other_ than the input is in the right language.
    assertEquals(0, rl.size());
  }

  /*
   * Check if the DFSA reachable from a given state is minimal. This means no
   * two states have the same right language.
   */
  public static void checkMinimal(final FSA fsa) {
    final HashMap<String, Integer> stateLanguages = new HashMap<String, Integer>();

    fsa.visitInPostOrder(new StateVisitor() {
      private StringBuilder b = new StringBuilder();

      public boolean accept(int state) {
        List<byte[]> rightLanguage = allSequences(fsa, state);
        Collections.sort(rightLanguage, FSABuilder.LEXICAL_ORDERING);

        b.setLength(0);
        for (byte[] seq : rightLanguage) {
          b.append(Arrays.toString(seq));
          b.append(',');
        }

        String full = b.toString();
        assertFalse("State exists: " + state + " " + full + " " + stateLanguages.get(full),
            stateLanguages.containsKey(full));
        stateLanguages.put(full, state);

        return true;
      }
    });
  }

  static List<byte[]> allSequences(FSA fsa, int state) {
    ArrayList<byte[]> seq = new ArrayList<byte[]>();
    for (ByteBuffer bb : fsa.getSequences(state)) {
      seq.add(Arrays.copyOf(bb.array(), bb.remaining()));
    }
    return seq;
  }

  /*
   * Check if two FSAs are identical.
   */
  public static void checkIdentical(FSA fsa1, FSA fsa2) {
    ArrayDeque<String> fromRoot = new ArrayDeque<String>();
    checkIdentical(fromRoot, fsa1, fsa1.getRootNode(), new BitSet(), fsa2, fsa2.getRootNode(), new BitSet());
  }

  /*
   * 
   */
  static void checkIdentical(ArrayDeque<String> fromRoot, FSA fsa1, int node1, BitSet visited1, FSA fsa2, int node2,
      BitSet visited2) {
    int arc1 = fsa1.getFirstArc(node1);
    int arc2 = fsa2.getFirstArc(node2);

    if (visited1.get(node1) != visited2.get(node2)) {
      throw new RuntimeException("Two nodes should either be visited or not visited: "
          + Arrays.toString(fromRoot.toArray()) + " " + " node1: " + node1 + " " + " node2: " + node2);
    }
    visited1.set(node1);
    visited2.set(node2);

    TreeSet<Character> labels1 = new TreeSet<Character>();
    TreeSet<Character> labels2 = new TreeSet<Character>();
    while (true) {
      labels1.add((char) fsa1.getArcLabel(arc1));
      labels2.add((char) fsa2.getArcLabel(arc2));

      arc1 = fsa1.getNextArc(arc1);
      arc2 = fsa2.getNextArc(arc2);

      if (arc1 == 0 || arc2 == 0) {
        if (arc1 != arc2) {
          throw new RuntimeException("Different number of labels at path: " + Arrays.toString(fromRoot.toArray()));
        }
        break;
      }
    }

    if (!labels1.equals(labels2)) {
      throw new RuntimeException("Different sets of labels at path: " + Arrays.toString(fromRoot.toArray()) + ":\n"
          + labels1 + "\n" + labels2);
    }

    // recurse.
    for (char chr : labels1) {
      byte label = (byte) chr;
      fromRoot.push(Character.isLetterOrDigit(chr) ? Character.toString(chr) : Integer.toString(chr));

      arc1 = fsa1.getArc(node1, label);
      arc2 = fsa2.getArc(node2, label);

      if (fsa1.isArcFinal(arc1) != fsa2.isArcFinal(arc2)) {
        throw new RuntimeException("Different final flag on arcs at: " + Arrays.toString(fromRoot.toArray())
            + ", label: " + label);
      }

      if (fsa1.isArcTerminal(arc1) != fsa2.isArcTerminal(arc2)) {
        throw new RuntimeException("Different terminal flag on arcs at: " + Arrays.toString(fromRoot.toArray())
            + ", label: " + label);
      }

      if (!fsa1.isArcTerminal(arc1)) {
        checkIdentical(fromRoot, fsa1, fsa1.getEndNode(arc1), visited1, fsa2, fsa2.getEndNode(arc2), visited2);
      }

      fromRoot.pop();
    }
  }

  /**
   * Return all matches for given regex
   */
  public static long regexQueryNrHits(String regex, FSA fsa) throws IOException {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, fsa);

    return resultList.size();
  }

  public static byte[][] convertToBytes(Set<String> strings) {
    byte[][] data = new byte[strings.size()][];

    Iterator<String> iterator = strings.iterator();

    int i = 0;
    while (iterator.hasNext()) {
      String string = iterator.next();
      data[i] = string.getBytes(Charset.defaultCharset());
      i++;
    }
    return data;
  }

  /**
   * Return all sequences reachable from a given node, as strings.
   */
  public static HashSet<String> suffixes(FSA fsa, int node) {
    HashSet<String> result = new HashSet<String>();
    for (ByteBuffer bb : fsa.getSequences(node)) {
      result.add(new String(bb.array(), bb.position(), bb.remaining(), UTF_8));
    }
    return result;
  }

  public static byte[][] convertToBytes(String[] strings) {
    byte[][] data = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      String string = strings[i];
      data[i] = string.getBytes(Charset.defaultCharset()); // you can chose charset
    }
    return data;
  }
}
