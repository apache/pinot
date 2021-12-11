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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.pinot.segment.local.utils.nativefst.builder.FSTBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.checkCorrect;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.generateRandom;
import static org.testng.Assert.assertEquals;


/**
 * Tests for the FSTBuilder
 */
public class FSTBuilderTest {
  private static byte[][] _input;
  private static byte[][] _input2;

  @BeforeClass
  public static void prepareByteInput() {
    _input = generateRandom(25000, new MinMax(1, 20), new MinMax(0, 255));
    _input2 = generateRandom(40, new MinMax(1, 20), new MinMax(0, 3));
  }

  @Test
  public void testEmptyInput() {
    byte[][] input = {};
    checkCorrect(input, FSTBuilder.build(input, new int[]{-1}));
  }

  @Test
  public void testHashResizeBug() {
    byte[][] input = {{0, 1}, {0, 2}, {1, 1}, {2, 1}};
    checkCorrect(input, FSTBuilder.build(input, new int[]{10, 11, 12, 13}));
  }

  @Test
  public void testSmallInput() {
    byte[][] input = {
        "abc".getBytes(StandardCharsets.UTF_8),
        "bbc".getBytes(StandardCharsets.UTF_8),
        "d".getBytes(StandardCharsets.UTF_8)
    };
    checkCorrect(input, FSTBuilder.build(input, new int[]{10, 11, 12}));
  }

  @Test
  public void testLexicographicOrder() {
    byte[][] input = {{0}, {1}, {(byte) 0xff}};
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);

    // Check if lexical ordering is consistent with absolute byte value.
    assertEquals(input[0][0], 0);
    assertEquals(input[1][0], 1);
    assertEquals(input[2][0], (byte) 0xff);

    FST fst = FSTBuilder.build(input, new int[]{10, 11, 12});
    checkCorrect(input, fst);

    int arc = fst.getFirstArc(fst.getRootNode());
    assertEquals(fst.getArcLabel(arc), 0);
    arc = fst.getNextArc(arc);
    assertEquals(fst.getArcLabel(arc), 1);
    arc = fst.getNextArc(arc);
    assertEquals(fst.getArcLabel(arc), (byte) 0xff);
  }

  @Test
  public void testRandom25000LargerAlphabet() {
    FST fst = FSTBuilder.build(_input, new int[]{10, 11, 12, 13});
    checkCorrect(_input, fst);
  }

  @Test
  public void testRandom25000SmallAlphabet() {
    FST fst = FSTBuilder.build(_input2, new int[]{10, 11, 12, 13});
    checkCorrect(_input2, fst);
  }
}
