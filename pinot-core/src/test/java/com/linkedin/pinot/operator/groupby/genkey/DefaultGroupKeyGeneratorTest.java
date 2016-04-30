/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.operator.groupby.genkey;

import com.linkedin.pinot.core.operator.groupby.DefaultGroupKeyGenerator;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;


/**
 * Test for group-by key generator.
 */
@Test
public class DefaultGroupKeyGeneratorTest {

  private Random _random;

  @BeforeSuite
  void init() {
    _random = new Random();
  }

  /**
   * Generate a group key using random values, decode the values back from
   * the key and assert that the original and decoded values are identical.
   */
  @Test
  void encodeDecodeTest() {
    int[] cardinalities = new int[]{9, 12, 234, 3456, 56789};
    int numValues = cardinalities.length;

    int[] values = new int[numValues];
    int[] decoded = new int[numValues];

    for (int i = 0; i < 10000; i++) {
      for (int j = 0; j < numValues; j++) {
        values[j] = Math.abs(_random.nextInt()) % cardinalities[j];
      }

      long groupKey = DefaultGroupKeyGenerator.generateRawKey(values, cardinalities);
      DefaultGroupKeyGenerator.decodeRawGroupKey(groupKey, cardinalities, decoded);

      for (int j = 0; j < numValues; j++) {
        Assert.assertEquals(values[j], decoded[j], "Actual : " + decoded[j] + " Expected: " + values[j]);
      }
    }
  }
}
