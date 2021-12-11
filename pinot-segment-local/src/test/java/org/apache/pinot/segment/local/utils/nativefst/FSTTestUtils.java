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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.roaringbitmap.RoaringBitmapWriter;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.FileAssert.fail;


/**
 * Test utils class
 */
class FSTTestUtils {
  private FSTTestUtils() {
  }

  /*
   * Generate a sorted list of random sequences.
   */
  public static byte[][] generateRandom(int count, MinMax length, MinMax alphabet) {
    byte[][] input = new byte[count][];
    Random rnd = new Random();
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
    byte[] bytes = new byte[length._min + rnd.nextInt(length.range())];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (alphabet._min + rnd.nextInt(alphabet.range()));
    }
    return bytes;
  }

  /*
   * Check if the DFST is correct with respect to the given input.
   */
  public static void checkCorrect(byte[][] input, FST fst) {
    // (1) All input sequences are in the right language.
    HashSet<ByteBuffer> rl = new HashSet<>();
    for (ByteBuffer bb : fst) {
      rl.add(ByteBuffer.wrap(Arrays.copyOf(bb.array(), bb.remaining())));
    }

    HashSet<ByteBuffer> uniqueInput = new HashSet<>();
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

  /**
   * Return number of matches for given regex
   */
  public static long regexQueryNrHits(String regex, FST fst) {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch(regex, fst, writer::add);

    return writer.get().getCardinality();
  }

  /**
   * Return all matches for given regex
   */
  public static List<Long> regexQueryNrHitsWithResults(String regex, FST fst) {
    RoaringBitmapWriter<MutableRoaringBitmap> writer = RoaringBitmapWriter.bufferWriter().get();
    RegexpMatcher.regexMatch(regex, fst, writer::add);
    MutableRoaringBitmap resultBitMap = writer.get();
    List<Long> resultList = new ArrayList<>();

    for (int dictId : resultBitMap) {
      resultList.add((long) dictId);
    }

    return resultList;
  }

  /**
   * Return all sequences reachable from a given node, as strings.
   */
  public static HashSet<String> suffixes(FST fst, int node) {
    HashSet<String> result = new HashSet<>();
    for (ByteBuffer bb : fst.getSequences(node)) {
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
