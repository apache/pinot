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
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class NumberGeneratorTest {

  @Test
  public void testNext() {
    Random random = mock(Random.class);
    when(random.nextInt(anyInt())).thenReturn(10); // initial value
    int cardinality = 5;
    double numValuesPerEntry = 1.0;

    // long generator
    NumberGenerator generator = new NumberGenerator(cardinality, FieldSpec.DataType.LONG, numValuesPerEntry, random);
    long[] expectedValues = { //
        10L, 11L, 12L, 13L, 14L, //
        10L, 11L, 12L, 13L, 14L
    };
    for (long expected : expectedValues) {
      assertEquals(generator.next(), expected);
    }

    // double generator
    generator = new NumberGenerator(cardinality, FieldSpec.DataType.FLOAT, numValuesPerEntry, random);
    float[] expectedValueFloat = { //
        10.5f, 11.5f, 12.5f, 13.5f, 14.5f, //
        10.5f, 11.5f, 12.5f, 13.5f, 14.5f
    };
    for (float expected : expectedValueFloat) {
      assertEquals(generator.next(), expected);
    }
  }

  @Test
  public void testNextMultiValued() {
    Random random = mock(Random.class);
    when(random.nextInt(anyInt())).thenReturn(10); // initial value
    when(random.nextDouble()).thenReturn(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9); // for MV generation
    int cardinality = 5;
    double numValuesPerEntry = 2.4;
    NumberGenerator generator = new NumberGenerator(cardinality, FieldSpec.DataType.INT, numValuesPerEntry, random);
    int[][] expectedValues = { //
        {10, 11, 12}, // rnd < 0.4
        {13, 14, 10}, // rnd < 0.4
        {11, 12, 13}, // rnd < 0.4
        {14, 10, 11}, // rnd < 0.4
        {12, 13}, // rnd >= 0.4
        {14, 10}, // rnd >= 0.4
        {11, 12}, // rnd >= 0.4
        {13, 14}, // rnd >= 0.4
        {10, 11}, // rnd >= 0.4
        {12, 13}, // rnd >= 0.4
    };
    for (int[] expected : expectedValues) {
      List<Integer> actual = (List<Integer>) generator.next();
      assertEquals(actual.toArray(), expected);
    }
  }
}
