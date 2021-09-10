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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.checkCorrect;
import static org.apache.pinot.segment.local.utils.nativefst.FSTTestUtils.generateRandom;
import static org.testng.Assert.assertEquals;


/**
 * Tests for the FSTBuilder
 */
public class FSTBuilderTest {
  private static byte[][] input;
  private static byte[][] input2;

  @BeforeClass
  public static void prepareByteInput() {
    input = generateRandom(25000, new MinMax(1, 20), new MinMax(0, 255));
    input2 = generateRandom(40, new MinMax(1, 20), new MinMax(0, 3));
  }

  @Test
  public void testEmptyInput() {
    byte[][] input = {};
    checkCorrect(input, FSTBuilder.build(input, new int[]{-1}));
  }

  @Test
  public void testHashResizeBug() {
    byte[][] input = {{0, 1}, {0, 2}, {1, 1}, {2, 1},};

    checkCorrect(input, FSTBuilder.build(input, new int[]{10, 11, 12, 13}));
  }

  @Test
  public void testSmallInput()
      throws Exception {
    byte[][] input = {"abc".getBytes(StandardCharsets.UTF_8), "bbc".getBytes(StandardCharsets.UTF_8), "d".getBytes(StandardCharsets.UTF_8),};
    checkCorrect(input, FSTBuilder.build(input, new int[]{10, 11, 12}));
  }

  @Test
  public void testLexicographicOrder()
      throws IOException {
    byte[][] input = {{0}, {1}, {(byte) 0xff},};
    Arrays.sort(input, FSTBuilder.LEXICAL_ORDERING);

    // Check if lexical ordering is consistent with absolute byte value.
    assertEquals(0, input[0][0]);
    assertEquals(1, input[1][0]);
    assertEquals((byte) 0xff, input[2][0]);

    final FST FST;
    checkCorrect(input, FST = FSTBuilder.build(input, new int[]{10, 11, 12}));

    int arc = FST.getFirstArc(FST.getRootNode());
    assertEquals(0, FST.getArcLabel(arc));
    arc = FST.getNextArc(arc);
    assertEquals(1, FST.getArcLabel(arc));
    arc = FST.getNextArc(arc);
    assertEquals((byte) 0xff, FST.getArcLabel(arc));
  }

  @Test
  public void testRandom25000_largerAlphabet() {
    FST FST = FSTBuilder.build(input, new int[]{10, 11, 12, 13});
    checkCorrect(input, FST);
  }

  @Test
  public void testRandom25000_smallAlphabet() {
    FST FST = FSTBuilder.build(input2, new int[]{10, 11, 12, 13});
    checkCorrect(input2, FST);
  }
}
