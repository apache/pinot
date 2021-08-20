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
package org.apache.pinot.segment.spi.partition;

import java.util.Random;
import org.apache.pinot.spi.utils.StringUtils;
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
      int expectedNumPartitions = Math.abs(random.nextInt());

      // Avoid divide-by-zero.
      if (expectedNumPartitions == 0) {
        expectedNumPartitions = 1;
      }

      String functionName = "MoDuLo";
      PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(functionName, expectedNumPartitions);
      Assert.assertEquals(partitionFunction.toString().toLowerCase(), functionName.toLowerCase());
      Assert.assertEquals(partitionFunction.getNumPartitions(), expectedNumPartitions);

      // Test int values.
      for (int j = 0; j < 1000; j++) {
        int value = random.nextInt();
        Assert.assertEquals(partitionFunction.getPartition(value), (value % expectedNumPartitions));
      }

      // Test long values.
      for (int j = 0; j < 1000; j++) {
        long value = random.nextLong();
        Assert.assertEquals(partitionFunction.getPartition(value), (value % expectedNumPartitions));
      }

      // Test Integer represented as String.
      for (int j = 0; j < 1000; j++) {
        int value = random.nextInt();
        Assert.assertEquals(partitionFunction.getPartition(Integer.toString(value)), (value % expectedNumPartitions));
      }

      // Test Long represented as String.
      for (int j = 0; j < 1000; j++) {
        long value = random.nextLong();
        Assert.assertEquals(partitionFunction.getPartition(Long.toString(value)), (value % expectedNumPartitions));
      }
    }
  }

  /**
   * Unit test for {@link MurmurPartitionFunction}.
   * <ul>
   *   <li> Tests that partition values are in expected range. </li>
   *   <li> Tests that toString returns expected string. </li>
   * </ul>
   */
  @Test
  public void testMurmurPartitioner() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < 1000; i++) {
      int expectedNumPartitions = Math.abs(random.nextInt());

      // Avoid divide-by-zero.
      if (expectedNumPartitions == 0) {
        expectedNumPartitions = 1;
      }

      String functionName = "mUrmur";

      PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(functionName, expectedNumPartitions);
      Assert.assertEquals(partitionFunction.toString().toLowerCase(), functionName.toLowerCase());
      Assert.assertEquals(partitionFunction.getNumPartitions(), expectedNumPartitions);

      for (int j = 0; j < 1000; j++) {
        int value = random.nextInt();
        String stringValue = Integer.toString(value);
        Assert.assertTrue(partitionFunction.getPartition(stringValue) < expectedNumPartitions,
            "Illegal: " + partitionFunction.getPartition(stringValue) + " " + expectedNumPartitions);
      }
    }
  }

  /**
   * Unit test for {@link MurmurPartitionFunction}.
   * <ul>
   *   <li> Tests that partition values are in expected range. </li>
   *   <li> Tests that toString returns expected string. </li>
   * </ul>
   */
  @Test
  public void testByteArrayPartitioner() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < 1000; i++) {
      int expectedNumPartitions = Math.abs(random.nextInt());

      // Avoid divide-by-zero.
      if (expectedNumPartitions == 0) {
        expectedNumPartitions = 1;
      }

      String functionName = "bYteArray";
      PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(functionName, expectedNumPartitions);
      Assert.assertEquals(partitionFunction.toString().toLowerCase(), functionName.toLowerCase());
      Assert.assertEquals(partitionFunction.getNumPartitions(), expectedNumPartitions);

      for (int j = 0; j < 1000; j++) {
        Integer value = random.nextInt();
        Assert.assertTrue(partitionFunction.getPartition(value) < expectedNumPartitions,
            "Illegal: " + partitionFunction.getPartition(value) + " " + expectedNumPartitions);
      }
    }
  }

  @Test
  public void testHashCodePartitioner() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < 1000; i++) {
      int expectedNumPartitions = Math.abs(random.nextInt());

      // Avoid divide-by-zero.
      if (expectedNumPartitions == 0) {
        expectedNumPartitions = 1;
      }

      String functionName = "HaShCoDe";
      PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(functionName, expectedNumPartitions);
      Assert.assertEquals(partitionFunction.toString().toLowerCase(), functionName.toLowerCase());
      Assert.assertEquals(partitionFunction.getNumPartitions(), expectedNumPartitions);

      for (int j = 0; j < 1000; j++) {
        Integer value = random.nextInt();
        Assert.assertEquals(partitionFunction.getPartition(value), Math.abs(value.hashCode()) % expectedNumPartitions);
      }
    }
  }

  /**
   * Tests the equivalence of org.apache.kafka.common.utils.Utils::murmur2 and {@link MurmurPartitionFunction::murmur2}
   * Our implementation of murmur2 has been copied over from Utils::murmur2
   */
  @Test
  public void testMurmurEquivalence() {

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.apache.kafka.common.utils.Utils::murmur2 to those values and stored in expectedMurmurValues
    int[] expectedMurmurValues =
        new int[]{-1044832774, -594851693, 1441878663, 1766739604, 1034724141, -296671913, 443511156, 1483601453, 1819695080, -931669296};

    long seed = 100;
    Random random = new Random(seed);

    int numPartitions = 5;
    MurmurPartitionFunction murmurPartitionFunction = new MurmurPartitionFunction(numPartitions);

    // Generate the same values as above - 10 random values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply {@link MurmurPartitionFunction::murmur2
    // compare with stored results
    byte[] array = new byte[7];
    for (int expectedMurmurValue : expectedMurmurValues) {
      random.nextBytes(array);
      int actualMurmurValue = murmurPartitionFunction.murmur2(array);
      Assert.assertEquals(actualMurmurValue, expectedMurmurValue);
    }
  }

  /**
   * Tests the equivalence of partitioning using org.apache.kafka.common.utils.Utils::partition and {@link MurmurPartitionFunction
   * ::getPartition}
   */
  @Test
  public void testMurmurPartitionFunctionEquivalence() {

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied {@link MurmurPartitionFunction} initialized with 5 partitions, by overriding {@MurmurPartitionFunction::murmur2} with org
    // .apache.kafka.common.utils.Utils::murmur2
    // stored the results in expectedPartitions
    int[] expectedPartitions = new int[]{1, 4, 4, 1, 1, 2, 0, 4, 2, 3};

    // initialized {@link MurmurPartitionFunction} with 5 partitions
    int numPartitions = 5;
    MurmurPartitionFunction murmurPartitionFunction = new MurmurPartitionFunction(numPartitions);

    // generate the same 10 String values
    // Apply the partition function and compare with stored results
    testPartitionFunctionEquivalence(murmurPartitionFunction, expectedPartitions);
  }

  /**
   * Tests the equivalence of kafka.producer.ByteArrayPartitioner::partition and {@link ByteArrayPartitionFunction ::getPartition}
   */
  @Test
  public void testByteArrayPartitionFunctionEquivalence() {
    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied kafka.producer.ByteArrayPartitioner::partition to those values and stored in expectedPartitions
    int[] expectedPartitions = new int[]{1, 3, 2, 0, 0, 4, 4, 1, 2, 4};

    // initialized {@link ByteArrayPartitionFunction} with 5 partitions
    int numPartitions = 5;
    ByteArrayPartitionFunction byteArrayPartitionFunction = new ByteArrayPartitionFunction(numPartitions);

    // generate the same 10 String values
    // Apply the partition function and compare with stored results
    testPartitionFunctionEquivalence(byteArrayPartitionFunction, expectedPartitions);
  }

  private void testPartitionFunctionEquivalence(PartitionFunction partitionFunction, int[] expectedPartitions) {
    long seed = 100;
    Random random = new Random(seed);

    // Generate 10 random String values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply given partition function
    // compare with expectedPartitions
    byte[] array = new byte[7];
    for (int expectedPartition : expectedPartitions) {
      random.nextBytes(array);
      String nextString = StringUtils.decodeUtf8(array);
      int actualPartition = partitionFunction.getPartition(nextString);
      Assert.assertEquals(actualPartition, expectedPartition);
    }
  }
}
