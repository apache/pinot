/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.query.transform;

import com.linkedin.pinot.core.operator.transform.function.SubtractionTransform;
import com.linkedin.pinot.core.operator.transform.function.TransformFunction;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for {@link SubtractionTransform} class.
 */
public class SubtractionTransformTest {

  private static final int NUM_ROWS = 1000;
  private static final double EPSILON = 1e-5;

  /**
   * Tests that the subtraction of two columns as returned by {@link SubtractionTransform
   * is as expected.
   */
  @Test
  public void test() {
    long seed = System.nanoTime();
    Random random = new Random(seed);

    double[] input1 = new double[NUM_ROWS];
    double[] input2 = new double[NUM_ROWS];

    double[] expected = new double[NUM_ROWS];

    for (int i = 0; i < NUM_ROWS; i++) {
      input1[i] = random.nextDouble();
      input2[i] = random.nextDouble();
      expected[i] = input1[i] - input2[i];
    }

    TransformFunction function = new SubtractionTransform();
    double[] actual = function.transform(NUM_ROWS, new TransformTestUtils.TestBlockValSet(input1, NUM_ROWS),
        new TransformTestUtils.TestBlockValSet(input2, NUM_ROWS));

    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actual[i], expected[i], EPSILON, ("Random seed: " + seed));
    }
  }
}
