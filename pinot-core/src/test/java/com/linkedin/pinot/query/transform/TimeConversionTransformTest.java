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

import com.linkedin.pinot.core.operator.docvalsets.ConstantBlockValSet;
import com.linkedin.pinot.core.operator.transform.function.TimeConversionTransform;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link TimeConversionTransform}.
 */
public class TimeConversionTransformTest {

  private static final int NUM_ROWS = 10001;

  @Test
  public void test() {
    long[] input = new long[NUM_ROWS];
    long[] expected = new long[NUM_ROWS];

    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROWS; i++) {
      input[i] = random.nextLong() % seed;
      expected[i] = TimeUnit.DAYS.convert(input[i], TimeUnit.MILLISECONDS);
    }

    TransformTestUtils.TestBlockValSet inputTimes = new TransformTestUtils.TestBlockValSet(input, NUM_ROWS);
    ConstantBlockValSet inputTimeUnit = new ConstantBlockValSet("MILLISECONDS", NUM_ROWS);
    ConstantBlockValSet outputTimeUnit = new ConstantBlockValSet("DAYS", NUM_ROWS);

    TimeConversionTransform function = new TimeConversionTransform();
    long[] actual = function.transform(NUM_ROWS, inputTimes, inputTimeUnit, outputTimeUnit);

    for (int i = 0; i < NUM_ROWS; i++) {
      Assert.assertEquals(actual[i], expected[i]);
    }
  }
}
