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
package org.apache.pinot.segment.local.utils.nativefst.org.apache.pinot.fsa;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.FSA;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.builders.FSABuilder;
import org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.builders.FSASerializer;
import org.testng.annotations.Test;

import static com.carrotsearch.randomizedtesting.RandomizedTest.*;
import static org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.FSAFlags.NUMBERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;


public abstract class SerializerTestBase extends TestBase {
  @Test
  public void testA() throws IOException {
    byte[][] input = new byte[][] { { 'a' }, };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input, new int[] {10});

    checkSerialization(input, s, true);
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
    FSA s = FSABuilder.build(input, new int[] {10, 11, 12, 13, 14});

    checkSerialization(input, s, true);
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
    FSA s = FSABuilder.build(input, new int[] {10, 11, 12, 13, 14});

    checkSerialization(input, s, true);
  }

  @Test
  public void testNotMinimal() throws IOException {
    byte[][] input = new byte[][] { 
      { 'a', 'b', 'a' }, 
      { 'b' },
      { 'b', 'a' }
    };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input, new int[] {10, 11, 12});

    checkSerialization(input, s, true);
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
    FSA s = FSABuilder.build(input, new int[] {10, 11, 12, 13});

    checkSerialization(input, s, true);
  }

  @Test
  public void testEmptyInput() throws IOException {
    byte[][] input = new byte[][] {};
    FSA s = FSABuilder.build(input, new int[] {10, 11, 12, 13});

    checkSerialization(input, s, true);
  }

  private void checkSerialization(byte[][] input, FSA root, boolean hasOutputSymbols) throws IOException {
    checkSerialization0(createSerializer(), input, root, hasOutputSymbols);
    if (createSerializer().getFlags().contains(NUMBERS)) {
      checkSerialization0(createSerializer().withNumbers(), input, root, hasOutputSymbols);
    }
  }

  private void checkSerialization(byte[][] input, FSA root) throws IOException {
    checkSerialization0(createSerializer(), input, root, false);
    if (createSerializer().getFlags().contains(NUMBERS)) {
      checkSerialization0(createSerializer().withNumbers(), input, root, false);
    }
  }

  private void checkSerialization0(FSASerializer serializer, final byte[][] in, FSA root, boolean hasOutputSymbols) throws IOException {
    final byte[] fsaData = serializer.serialize(root, new ByteArrayOutputStream()).toByteArray();

    FSA fsa = FSA.read(new ByteArrayInputStream(fsaData), hasOutputSymbols,
        new DirectMemoryManager(SerializerTestBase.class.getName()));
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
    byte[][] input = new byte[][] { 
      { 'a' }, 
      { 'a', 'b', 'a' },
      { 'a', 'c' }, 
      { 'b' }, 
      { 'b', 'a' }, 
      { 'c' }, };

    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);
    FSA s = FSABuilder.build(input, new int[] {10, 11, 12, 13});

    final byte[] fsaData = 
        createSerializer().withNumbers()
                          .serialize(s, new ByteArrayOutputStream())
                          .toByteArray();

    FSA fsa = FSA.read(new ByteArrayInputStream(fsaData), true,
        new DirectMemoryManager(SerializerTestBase.class.getName()));

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
