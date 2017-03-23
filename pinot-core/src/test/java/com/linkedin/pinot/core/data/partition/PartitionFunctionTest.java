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
package com.linkedin.pinot.core.data.partition;

import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit test for {@link PartitionFunction}
 */
public class PartitionFunctionTest {

  /**
   * Unit test for {@link ModuloPartitionFunction}.
   * <ul>
   *   <li> Builds an instance of the {@link ModuloPartitionFunction}. </li>
   *   <li> Performs modulo operations on random numbers and asserts results returned by the partition function
   *        are as expected. </li>
   * </ul>
   */
  @Test
  public void testModulo() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < 1000; i++) {
      int divisor = random.nextInt();
      String partitionFunctionString = "MoDuLo" + PartitionFunctionFactory.PARTITION_FUNCTION_DELIMITER + divisor;
      PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(partitionFunctionString);

      for (int j = 0; j < 1000; j++) {
        int value = random.nextInt();
        Assert.assertEquals(partitionFunction.getPartition(value), (value % divisor));
      }
    }
  }
}
