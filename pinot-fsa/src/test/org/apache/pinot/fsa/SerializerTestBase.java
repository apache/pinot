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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.pinot.fsa.builders.FSABuilder;
import org.apache.pinot.fsa.builders.FSASerializer;
import org.junit.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;
import static org.apache.pinot.fsa.FSAFlags.NUMBERS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public abstract class SerializerTestBase extends TestBase {
  @Test
  public void testA() throws IOException {
    byte[][] input = new byte[][] { { 'a' }, };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input);

    checkSerialization(input, s);
  }

  @Test
  public void testArcsSharing() throws IOException {
    byte[][] input = new byte[][] { 
      { 'a', 'c', 'f' },
      { 'a', 'd', 'g' },
      { 'a', 'e', 'h' },
      { 'b', 'd', 'g' },
      { 'b', 'e', 'h' },
    };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input);

    checkSerialization(input, s);
  }

  @Test
  public void testFSA5SerializerSimple() throws IOException {
    byte[][] input = new byte[][] { 
      { 'a' }, 
      { 'a', 'b', 'a' },
      { 'a', 'c' }, 
      { 'b' }, 
      { 'b', 'a' }, 
      { 'c' },
    };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input);

    checkSerialization(input, s);
  }

  @Test
  public void testNotMinimal() throws IOException {
    byte[][] input = new byte[][] { 
      { 'a', 'b', 'a' }, 
      { 'b' },
      { 'b', 'a' }
    };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input);

    checkSerialization(input, s);
  }

  @Test
  public void testFSA5Bug0() throws IOException {
    checkCorrect(new String[] { 
      "3-D+A+JJ", 
      "3-D+A+NN", 
      "4-F+A+NN",
      "z+A+NN", });
  }

  @Test
  public void testFSA5Bug1() throws IOException {
    checkCorrect(new String[] { "+NP", "n+N", "n+NP", });
  }

  private void checkCorrect(String[] strings) throws IOException {
    byte[][] input = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
        input[i] = strings[i].getBytes("ISO8859-1");
    }

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input);

    checkSerialization(input, s);
  }

  @Test
  public void testEmptyInput() throws IOException {
    byte[][] input = new byte[][] {};
    FSA s = FSABuilder.build(input);

    checkSerialization(input, s);
  }

  @Test
  public void test_abc() throws IOException {
    testBuiltIn(FSA.read(FSA5Test.class.getResourceAsStream("/resources/abc.fsa")));
  }

  @Test
  public void test_minimal() throws IOException {
    testBuiltIn(FSA.read(FSA5Test.class.getResourceAsStream("/resources/minimal.fsa")));
  }

  @Test
  public void test_minimal2() throws IOException {
    testBuiltIn(FSA.read(FSA5Test.class.getResourceAsStream("/resources/minimal2.fsa")));
  }

  @Test
  public void test_en_tst() throws IOException {
    testBuiltIn(FSA.read(FSA5Test.class.getResourceAsStream("/resources/en_tst.dict")));
  }

  private void testBuiltIn(FSA fsa) throws IOException {
    final ArrayList<byte[]> sequences = new ArrayList<byte[]>();

    sequences.clear();
    for (ByteBuffer bb : fsa) {
      sequences.add(Arrays.copyOf(bb.array(), bb.remaining()));
    }

    Collections.sort(sequences, FSABuilder.LEXICAL_ORDERING);

    final byte[][] in = sequences.toArray(new byte[sequences.size()][]);
    FSA root = FSABuilder.build(in);

    // Check if the DFSA is correct first.
    FSATestUtils.checkCorrect(in, root);

    // Check serialization.
    checkSerialization(in, root);
  }

  private void checkSerialization(byte[][] input, FSA root) throws IOException {
    checkSerialization0(createSerializer(), input, root);
    if (createSerializer().getFlags().contains(NUMBERS)) {
      checkSerialization0(createSerializer().withNumbers(), input, root);
    }
  }

  private void checkSerialization0(FSASerializer serializer, final byte[][] in, FSA root) throws IOException {
    final byte[] fsaData = serializer.serialize(root, new ByteArrayOutputStream()).toByteArray();

    FSA fsa = FSA.read(new ByteArrayInputStream(fsaData));
    checkCorrect(in, fsa);
  }

  /*
   * Check if the FSA is correct with respect to the given input.
   */
  protected void checkCorrect(byte[][] input, FSA fsa) {
    // (1) All input sequences are in the right language.
    HashSet<ByteBuffer> rl = new HashSet<ByteBuffer>();
    for (ByteBuffer bb : fsa) {
      byte[] array = bb.array();
      int length = bb.remaining();
      rl.add(ByteBuffer.wrap(Arrays.copyOf(array, length)));
    }

    HashSet<ByteBuffer> uniqueInput = new HashSet<ByteBuffer>();
    for (byte[] sequence : input) {
      uniqueInput.add(ByteBuffer.wrap(sequence));
    }

    for (ByteBuffer sequence : uniqueInput) {
      if (!rl.remove(sequence)) {
        fail("Not present in the right language: " + toString(sequence));
      }
    }

    // (2) No other sequence _other_ than the input is in the right
    // language.
    assertEquals(0, rl.size());
  }

  @Test
  public void testAutomatonWithNodeNumbers() throws IOException {
    assumeTrue(createSerializer().getFlags().contains(NUMBERS));

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
        createSerializer().withNumbers()
                          .serialize(s, new ByteArrayOutputStream())
                          .toByteArray();

    FSA fsa = FSA.read(new ByteArrayInputStream(fsaData));

    // Ensure we have the NUMBERS flag set.
    assertTrue(fsa.getFlags().contains(NUMBERS));

    // Get all numbers from nodes.
    byte[] buffer = new byte[128];
    final ArrayList<String> result = new ArrayList<String>();
    FSA5Test.walkNode(buffer, 0, fsa, fsa.getRootNode(), 0, result);

    Collections.sort(result);
    assertEquals(
            Arrays.asList("0 a", "1 aba", "2 ac", "3 b", "4 ba", "5 c"),
            result);
  }

  protected abstract FSASerializer createSerializer();

  /*
   * Drain bytes from a byte buffer to a string.
   */
  public static String toString(ByteBuffer sequence) {
      byte [] bytes = new byte [sequence.remaining()];
      sequence.get(bytes);
      return Arrays.toString(bytes);
  }    
}
