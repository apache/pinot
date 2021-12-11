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
package org.apache.pinot.core.query.aggregation;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for DoubleAggregationResultHolder class.
 */
@Test
public class DoubleAggregationResultHolderTest {
  private static final long RANDOM_SEED = System.nanoTime();
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final double DEFAULT_VALUE = RANDOM.nextDouble();

  @Test
  void testDefaultValue() {
    AggregationResultHolder resultHolder = new DoubleAggregationResultHolder(DEFAULT_VALUE);

    double actual = resultHolder.getDoubleResult();
    Assert.assertEquals(actual, DEFAULT_VALUE,
        "Default Value mismatch: Actual: " + actual + " Expected: " + DEFAULT_VALUE + " Random seed: " + RANDOM_SEED);
  }

  /**
   * This test is for the AggregationResultHolder.SetValue() api.
   * - Sets a random value in the result holder.
   * - Asserts that the value returned by the result holder is as expected.
   */
  @Test
  void testSetValue() {
    AggregationResultHolder resultHolder = new DoubleAggregationResultHolder(DEFAULT_VALUE);

    for (int i = 0; i < 1000; i++) {
      double expected = RANDOM.nextDouble();
      resultHolder.setValue(expected);
      double actual = resultHolder.getDoubleResult();

      Assert.assertEquals(actual, expected,
          "Mis-match: Actual: " + actual + " Expected: " + expected + " Random seed: " + RANDOM_SEED);
    }
  }
}
