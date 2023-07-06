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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTSerializer;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTFlags.NUMBERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;


/**
 * Base class for serializer tests
 */
public abstract class SerializerTestBase {
  /*
   * Drain bytes from a byte buffer to a string.
   */
  public static String toString(ByteBuffer sequence) {
    byte[] bytes = new byte[sequence.remaining()];
    sequence.get(bytes);
    return Arrays.toString(bytes);
  }

  @Test
  public void testA()
      throws IOException {
    byte[][] input = new byte[][]{{'a'}};
    FST s = FSTBuilder.build(input, new int[]{10});

    checkSerialization(input, s, true);
  }

  @Test
  public void testArcsSharing()
      throws IOException {
    byte[][] input = new byte[][]{
        {'a', 'c', 'f'}, {'a', 'd', 'g'}, {'a', 'e', 'h'}, {'b', 'd', 'g'}, {'b', 'e', 'h'}
    };
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);
    FST s = FSTBuilder.build(input, new int[]{10, 11, 12, 13, 14});

    checkSerialization(input, s, true);
  }

  @Test
  public void testImmutableFSTSerializerSimple()
      throws IOException {
    byte[][] input = new byte[][]{{'a'}, {'a', 'b', 'a'}, {'a', 'c'}, {'b'}, {'b', 'a'}, {'c'}};
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);
    FST fst = FSTBuilder.build(input, new int[]{10, 11, 12, 13, 14});

    checkSerialization(input, fst, true);
  }

  @Test
  public void testNotMinimal()
      throws IOException {
    byte[][] input = new byte[][]{{'a', 'b', 'a'}, {'b'}, {'b', 'a'}};
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);
    FST fst = FSTBuilder.build(input, new int[]{10, 11, 12});

    checkSerialization(input, fst, true);
  }

  @Test
  public void testImmutableFSTBug0()
      throws IOException {
    checkCorrect(new String[]{"3-D+A+JJ", "3-D+A+NN", "4-F+A+NN", "z+A+NN"});
  }

  @Test
  public void testImmutableFSTBug1()
      throws IOException {
    checkCorrect(new String[]{"+NP", "n+N", "n+NP"});
  }

  private void checkCorrect(String[] strings)
      throws IOException {
    byte[][] input = new byte[strings.length][];
    for (int i = 0; i < strings.length; i++) {
      input[i] = strings[i].getBytes("ISO8859-1");
    }
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);
    FST fst = FSTBuilder.build(input, new int[]{10, 11, 12, 13});

    checkSerialization(input, fst, true);
  }

  @Test
  public void testEmptyInput()
      throws IOException {
    byte[][] input = new byte[][]{};
    FST fst = FSTBuilder.build(input, new int[]{10, 11, 12, 13});

    checkSerialization(input, fst, true);
  }

  private void checkSerialization(byte[][] input, FST root, boolean hasOutputSymbols)
      throws IOException {
    checkSerialization0(createSerializer(), input, root, hasOutputSymbols);
    if (createSerializer().getFlags().contains(NUMBERS)) {
      checkSerialization0(createSerializer().withNumbers(), input, root, hasOutputSymbols);
    }
  }

  private void checkSerialization0(FSTSerializer serializer, final byte[][] in, FST root, boolean hasOutputSymbols)
      throws IOException {
    byte[] fstData = serializer.serialize(root, new ByteArrayOutputStream()).toByteArray();
    //TODO: Atri
    FST fst = FST.read(new ByteArrayInputStream(fstData), hasOutputSymbols,
        new DirectMemoryManager(SerializerTestBase.class.getName()), 0);
    checkCorrect(in, fst);
  }

  /*
   * Check if the FST is correct with respect to the given input.
   */
  protected void checkCorrect(byte[][] input, FST fst) {
    // (1) All input sequences are in the right language.
    HashSet<ByteBuffer> rl = new HashSet<>();
    for (ByteBuffer bb : fst) {
      byte[] array = bb.array();
      int length = bb.remaining();
      rl.add(ByteBuffer.wrap(Arrays.copyOf(array, length)));
    }

    HashSet<ByteBuffer> uniqueInput = new HashSet<>();
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
    assertEquals(rl.size(), 0);
  }

  @Test
  public void testAutomatonWithNodeNumbers()
      throws IOException {
    byte[][] input = new byte[][]{{'a'}, {'a', 'b', 'a'}, {'a', 'c'}, {'b'}, {'b', 'a'}, {'c'}};
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);

    FST fst = FSTBuilder.build(input, new int[]{10, 11, 12, 13});
    byte[] fstData = createSerializer().withNumbers().serialize(fst, new ByteArrayOutputStream()).toByteArray();
    //TODO: Atri
    fst =
        FST.read(new ByteArrayInputStream(fstData), true, new DirectMemoryManager(SerializerTestBase.class.getName()), 0);

    // Ensure we have the NUMBERS flag set.
    assertTrue(fst.getFlags().contains(NUMBERS));

    // Get all numbers from nodes.
    byte[] buffer = new byte[128];
    List<String> result = new ArrayList<>();
    ImmutableFSTTest.walkNode(buffer, 0, fst, fst.getRootNode(), 0, result);

    result.sort(null);
    assertEquals(result, Arrays.asList("0 a", "1 aba", "2 ac", "3 b", "4 ba", "5 c"));
  }

  protected abstract FSTSerializer createSerializer();
}
