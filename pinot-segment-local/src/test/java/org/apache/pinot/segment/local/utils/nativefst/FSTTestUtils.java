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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.FileAssert.fail;


/**
 * Test utils class
 */
public class FSTTestUtils {
  /*
   * Generate a sorted list of random sequences.
   */
  public static byte[][] generateRandom(int count, MinMax length, MinMax alphabet) {
    final byte[][] input = new byte[count][];
    final Random rnd = new Random();
    for (int i = 0; i < count; i++) {
      input[i] = randomByteSequence(rnd, length, alphabet);
    }
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);
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
   * Check if the DFST is correct with respect to the given input.
   */
  public static void checkCorrect(byte[][] input, FST FST) {
    // (1) All input sequences are in the right language.
    HashSet<ByteBuffer> rl = new HashSet<ByteBuffer>();
    for (ByteBuffer bb : FST) {
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
   * 
   */
  static void checkIdentical(ArrayDeque<String> fromRoot, FST FST1, int node1, BitSet visited1, FST FST2, int node2,
      BitSet visited2) {
    int arc1 = FST1.getFirstArc(node1);
    int arc2 = FST2.getFirstArc(node2);

    if (visited1.get(node1) != visited2.get(node2)) {
      throw new RuntimeException("Two nodes should either be visited or not visited: "
          + Arrays.toString(fromRoot.toArray()) + " " + " node1: " + node1 + " " + " node2: " + node2);
    }
    visited1.set(node1);
    visited2.set(node2);

    TreeSet<Character> labels1 = new TreeSet<Character>();
    TreeSet<Character> labels2 = new TreeSet<Character>();
    while (true) {
      labels1.add((char) FST1.getArcLabel(arc1));
      labels2.add((char) FST2.getArcLabel(arc2));

      arc1 = FST1.getNextArc(arc1);
      arc2 = FST2.getNextArc(arc2);

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

      arc1 = FST1.getArc(node1, label);
      arc2 = FST2.getArc(node2, label);

      if (FST1.isArcFinal(arc1) != FST2.isArcFinal(arc2)) {
        throw new RuntimeException("Different final flag on arcs at: " + Arrays.toString(fromRoot.toArray())
            + ", label: " + label);
      }

      if (FST1.isArcTerminal(arc1) != FST2.isArcTerminal(arc2)) {
        throw new RuntimeException("Different terminal flag on arcs at: " + Arrays.toString(fromRoot.toArray())
            + ", label: " + label);
      }

      if (!FST1.isArcTerminal(arc1)) {
        checkIdentical(fromRoot, FST1, FST1.getEndNode(arc1), visited1, FST2, FST2.getEndNode(arc2), visited2);
      }

      fromRoot.pop();
    }
  }

  /**
   * Return number of matches for given regex
   */
  public static long regexQueryNrHits(String regex, FST FST) {
    return RegexpMatcher.regexMatch(regex, FST).size();
  }

  /**
   * Return all matches for given regex
   */
  public static List<Long> regexQueryNrHitsWithResults(String regex, FST FST) {
    return RegexpMatcher.regexMatch(regex, FST);
  }

  /**
   * Return all sequences reachable from a given node, as strings.
   */
  public static HashSet<String> suffixes(FST FST, int node) {
    HashSet<String> result = new HashSet<String>();
    for (ByteBuffer bb : FST.getSequences(node)) {
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

  public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
    return new HashSet<>(list1).equals(new HashSet<>(list2));
  }
}
