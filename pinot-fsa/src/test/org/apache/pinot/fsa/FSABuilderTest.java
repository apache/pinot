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

import static org.apache.pinot.fsa.FSATestUtils.checkMinimal;
import static org.apache.pinot.fsa.FSATestUtils.generateRandom;
import static org.apache.pinot.fsa.FSATestUtils.checkCorrect;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pinot.fsa.builders.FSABuilder;
import org.junit.BeforeClass;
import org.junit.Test;

public class FSABuilderTest extends TestBase {
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
    checkCorrect(input, FSABuilder.build(input, new int[] {-1}));
  }

  @Test
  public void testHashResizeBug() {
    byte[][] input = { { 0, 1 }, { 0, 2 }, { 1, 1 }, { 2, 1 }, };

    FSA fsa = FSABuilder.build(input, new int[] {10, 11, 12, 13});
    checkCorrect(input, FSABuilder.build(input, new int[] {10, 11, 12, 13}));
    checkMinimal(fsa);
  }

  @Test
  public void testSmallInput() throws Exception {
    byte[][] input = { "abc".getBytes("UTF-8"), "bbc".getBytes("UTF-8"), "d".getBytes("UTF-8"), };
    checkCorrect(input, FSABuilder.build(input, new int[] {10, 11, 12}));
  }

  @Test
  public void testLexicographicOrder() throws IOException {
    byte[][] input = { { 0 }, { 1 }, { (byte) 0xff }, };
    Arrays.sort(input, FSABuilder.LEXICAL_ORDERING);

    // Check if lexical ordering is consistent with absolute byte value.
    assertEquals(0, input[0][0]);
    assertEquals(1, input[1][0]);
    assertEquals((byte) 0xff, input[2][0]);

    final FSA fsa;
    checkCorrect(input, fsa = FSABuilder.build(input, new int[] {10, 11, 12}));

    int arc = fsa.getFirstArc(fsa.getRootNode());
    assertEquals(0, fsa.getArcLabel(arc));
    arc = fsa.getNextArc(arc);
    assertEquals(1, fsa.getArcLabel(arc));
    arc = fsa.getNextArc(arc);
    assertEquals((byte) 0xff, fsa.getArcLabel(arc));
  }

  @Test
  public void testRandom25000_largerAlphabet() {
    FSA fsa = FSABuilder.build(input, new int[] {10, 11, 12, 13});
    checkCorrect(input, fsa);
    checkMinimal(fsa);
  }

  @Test
  public void testRandom25000_smallAlphabet() throws IOException {
    FSA fsa = FSABuilder.build(input2, new int[] {10, 11, 12, 13});
    checkCorrect(input2, fsa);
    checkMinimal(fsa);
  }
}
