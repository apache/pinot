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

package org.apache.pinot.controller.recommender.data.generator;

import java.util.List;
import java.util.Random;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class BooleanGeneratorTest {

  @Test
  public void testNext() {
    Random random = mock(Random.class);
    when(random.nextBoolean()).thenReturn(false, true, false, false, true, true, false, true, false, true);

    // long generator
    BooleanGenerator generator = new BooleanGenerator(1.0, random);
    int[] expectedValues = { //
        0, 1, 0, 0, 1, //
        1, 0, 1, 0, 1
    };
    for (int expected : expectedValues) {
      assertEquals(generator.next(), expected);
    }
  }

  @Test
  public void testNextMultiValued() {
    Random random = mock(Random.class);
    when(random.nextBoolean())
        .thenReturn(false, true, false, false, false, true, true, true, true, false, true, false, false, true, true,
            false, false, false, true, true, false, true, true, true);
    when(random.nextDouble()).thenReturn(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9); // for MV generation

    double numValuesPerEntry = 2.4;
    BooleanGenerator generator = new BooleanGenerator(numValuesPerEntry, random);
    int[][] expectedValues = {
        {0, 1, 0}, // rnd < 0.4
        {0, 0, 1}, // rnd < 0.4
        {1, 1, 1}, // rnd < 0.4
        {0, 1, 0}, // rnd < 0.4
        {0, 1},  // rnd >= 0.4
        {1, 0},  // rnd >= 0.4
        {0, 0},  // rnd >= 0.4
        {1, 1},  // rnd >= 0.4
        {0, 1},  // rnd >= 0.4
        {1, 1},  // rnd >= 0.4
    };
    for (int[] expected : expectedValues) {
      List<Integer> actual = (List<Integer>) generator.next();
      assertEquals(actual.toArray(), expected);
    }
  }
}
