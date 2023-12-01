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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit test for {@link PartitionFunction}
 */
public class PartitionFunctionTest {
  private static final int NUM_ROUNDS = 1000;
  private static final int MAX_NUM_PARTITIONS = 100;

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

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;

      String functionName = "MoDuLo";
      PartitionFunction partitionFunction =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);

      testBasicProperties(partitionFunction, functionName, numPartitions);

      // Test int values
      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();
        int expectedPartition = value % numPartitions;
        if (expectedPartition < 0) {
          expectedPartition += numPartitions;
        }
        assertEquals(partitionFunction.getPartition(value), expectedPartition);
        assertEquals(partitionFunction.getPartition((long) value), expectedPartition);
        assertEquals(partitionFunction.getPartition(Integer.toString(value)), expectedPartition);
      }

      // Test long values
      for (int j = 0; j < NUM_ROUNDS; j++) {
        long value = j == 0 ? Long.MIN_VALUE : random.nextLong();
        int expectedPartition = (int) (value % numPartitions);
        if (expectedPartition < 0) {
          expectedPartition += numPartitions;
        }
        assertEquals(partitionFunction.getPartition(value), expectedPartition);
        assertEquals(partitionFunction.getPartition(Long.toString(value)), expectedPartition);
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

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;

      String functionName = "mUrmur";
      PartitionFunction partitionFunction =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);

      testBasicProperties(partitionFunction, functionName, numPartitions);

      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();
        int partition1 = partitionFunction.getPartition(value);
        int partition2 = partitionFunction.getPartition(Integer.toString(value));
        assertEquals(partition1, partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);
      }
    }
  }

  /**
   * Unit test for {@link Murmur3PartitionFunction}.
   * <ul>
   *   <li> Tests that partition values are in expected range. </li>
   *   <li> Tests that toString returns expected string. </li>
   *   <li> Tests that the partition numbers returned by partition function with null functionConfig and
   *   functionConfig with empty seed value are equal</li>
   * </ul>
   */
  @Test
  public void testMurmur3Partitioner() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;

      String functionName = "MurMUr3";
      String valueTobeHashed = String.valueOf(random.nextInt());
      Map<String, String> functionConfig = new HashMap<>();

      // Create partition function with function config as null.
      PartitionFunction partitionFunction1 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);

      // Check getName and toString equivalence.
      assertEquals(partitionFunction1.getName(), partitionFunction1.toString());

      // Get partition number with random value.
      int partitionNumWithNullConfig = partitionFunction1.getPartition(valueTobeHashed);

      // Create partition function with function config present but no seed value present.
      PartitionFunction partitionFunction2 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, functionConfig);

      // Get partition number with random value.
      int partitionNumWithNoSeedValue = partitionFunction2.getPartition(valueTobeHashed);

      // The partition number with null function config and function config with empty seed value should be equal.
      assertEquals(partitionNumWithNullConfig, partitionNumWithNoSeedValue);

      // Put random seed value in "seed" field in the function config.
      functionConfig.put("seed", Integer.toString(random.nextInt()));

      // Create partition function with function config present but random seed value present in function config.
      PartitionFunction partitionFunction3 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, functionConfig);

      // Create partition function with function config present with random seed value
      // and with variant provided as "x64_32" in function config.
      functionConfig.put("variant", "x64_32");
      PartitionFunction partitionFunction4 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, functionConfig);

      testBasicProperties(partitionFunction1, functionName, numPartitions);
      testBasicProperties(partitionFunction2, functionName, numPartitions);
      testBasicProperties(partitionFunction3, functionName, numPartitions);
      testBasicProperties(partitionFunction4, functionName, numPartitions);

      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();

        // check for the partition function with function config as null.
        int partition1 = partitionFunction1.getPartition(value);
        int partition2 = partitionFunction1.getPartition(Integer.toString(value));
        assertEquals(partition1, partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);

        // check for the partition function with non-null function config but without seed value.
        partition1 = partitionFunction2.getPartition(value);
        partition2 = partitionFunction2.getPartition(Integer.toString(value));
        assertEquals(partition1, partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);

        // check for the partition function with non-null function config and with seed value.
        partition1 = partitionFunction3.getPartition(value);
        partition2 = partitionFunction3.getPartition(Integer.toString(value));
        assertEquals(partition1, partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);

        // check for the partition function with non-null function config and with seed value and variant.
        partition1 = partitionFunction4.getPartition(value);
        partition2 = partitionFunction4.getPartition(Integer.toString(value));
        assertEquals(partition1, partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);
      }
    }
  }

  @Test
  public void testMurmur3Equivalence() {
    long seed = 100;
    int numberOfPartitions = 5;
    Random random = new Random(seed);

    Murmur3PartitionFunction murmur3PartitionFunction = new Murmur3PartitionFunction(numberOfPartitions, null);

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeed.

    int[] expectedMurmurValuesFor32BitX64WithZeroSeed = new int[]{
        -1569442405, -921191038, 16439113, -881572510, 2111401876, 655879980, 1409856380, -1348364123, -1770645361,
        1277101955
    };

    // Generate the same values as above - 10 random values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply {@link Murmur3PartitionFunction::murmurHash332bitsX64}. Here seed = 0.
    // compare with stored results.
    byte[] array = new byte[7];
    for (int expectedMurmur3Value : expectedMurmurValuesFor32BitX64WithZeroSeed) {
      random.nextBytes(array);
      int actualMurmurValue = murmur3PartitionFunction.murmurHash332bitsX64(array, 0);
      assertEquals(actualMurmurValue, expectedMurmur3Value);
    }

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 9001 to those values and
    // stored in expectedMurmurValuesFor32BitX64WithNonZeroSeed.

    int[] expectedMurmurValuesFor32BitX64WithNonZeroSeed = new int[]{
        698083240, 174075836, -938825597, 155806634, -831733828, 319389887, -939822329, -1785781936, -1796939240,
        757512622
    };

    // Generate the same values as above - 10 random values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply {@link Murmur3PartitionFunction::murmurHash332bitsX64}. Here seed = 9001.
    // compare with stored results.
    random = new Random(seed);
    array = new byte[7];
    for (int expectedMurmur3Value : expectedMurmurValuesFor32BitX64WithNonZeroSeed) {
      random.nextBytes(array);
      int actualMurmurValue = murmur3PartitionFunction.murmurHash332bitsX64(array, 9001);
      assertEquals(actualMurmurValue, expectedMurmur3Value);
    }

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied com.google.common.hash.hashing::murmur3_32_fixed to those values with seed = 0 and stored in
    // expectedMurmurValuesFor32BitX86WithZeroSeed.
    int[] expectedMurmurValuesFor32BitX86WithZeroSeed = new int[]{
        1255034832, -395463542, 659973067, 1070436837, -1193041642, -1412829846, -483463488, -1385092001, 568671606,
        -807299446,
    };

    // Generate the same values as above - 10 random values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply {@link Murmur3PartitionFunction::murmurHash332bitsX86}. Here seed = 0.
    // compare with stored results.
    random = new Random(seed);
    for (int expectedMurmur3Value : expectedMurmurValuesFor32BitX86WithZeroSeed) {
      random.nextBytes(array);
      int actualMurmurValue = murmur3PartitionFunction.murmurHash332bitsX86(array, 0);
      assertEquals(actualMurmurValue, expectedMurmur3Value);
    }

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied com.google.common.hash.hashing::murmur3_32_fixed to those values with seed = 9001 and stored in
    // expectedMurmurValuesFor32BitX86WithNonZeroSeed.
    int[] expectedMurmurValuesFor32BitX86WithNonZeroSeed = new int[]{
        -590969347, -315366997, 1642137565, -1732240651, -597560989, -1430018124, -448506674, 410998174, -1912106487,
        -19253806,
    };

    // Generate the same values as above - 10 random values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply {@link Murmur3PartitionFunction::murmurHash332bitsX86}. Here seed = 9001.
    // compare with stored results
    random = new Random(seed);
    for (int expectedMurmur3Value : expectedMurmurValuesFor32BitX86WithNonZeroSeed) {
      random.nextBytes(array);
      int actualMurmurValue = murmur3PartitionFunction.murmurHash332bitsX86(array, 9001);
      assertEquals(actualMurmurValue, expectedMurmur3Value);
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

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;

      String functionName = "bYteArray";
      PartitionFunction partitionFunction =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);

      testBasicProperties(partitionFunction, functionName, numPartitions);

      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();
        int partition1 = partitionFunction.getPartition(value);
        int partition2 = partitionFunction.getPartition(Integer.toString(value));
        assertEquals(partition1, partition2);
        assertTrue(partition1 >= 0 && partition1 < numPartitions);
      }
    }
  }

  @Test
  public void testHashCodePartitioner() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;

      String functionName = "HaShCoDe";
      PartitionFunction partitionFunction =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);

      testBasicProperties(partitionFunction, functionName, numPartitions);

      // Test int values
      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();
        int hashCode = Integer.toString(value).hashCode();
        int expectedPartition = ((hashCode == Integer.MIN_VALUE) ? 0 : Math.abs(hashCode)) % numPartitions;
        assertEquals(partitionFunction.getPartition(value), expectedPartition);
        assertEquals(partitionFunction.getPartition(Integer.toString(value)), expectedPartition);
      }

      // Test double values
      for (int j = 0; j < NUM_ROUNDS; j++) {
        double value = j == 0 ? Double.NEGATIVE_INFINITY : random.nextDouble();
        int hashCode = Double.toString(value).hashCode();
        int expectedPartition = ((hashCode == Integer.MIN_VALUE) ? 0 : Math.abs(hashCode)) % numPartitions;
        assertEquals(partitionFunction.getPartition(value), expectedPartition);
        assertEquals(partitionFunction.getPartition(Double.toString(value)), expectedPartition);
      }
    }
  }

  @Test
  public void testBoundedColumnValuePartitioner() {
    String functionName = "BOUndedColumNVaLUE";
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("columnValues", "Maths|english|Chemistry");
    functionConfig.put("columnValuesDelimiter", "|");
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction(functionName, 4, functionConfig);
    testBasicProperties(partitionFunction, functionName, 4, functionConfig);
    assertEquals(partitionFunction.getPartition("maths"), 1);
    assertEquals(partitionFunction.getPartition("English"), 2);
    assertEquals(partitionFunction.getPartition("Chemistry"), 3);
    assertEquals(partitionFunction.getPartition("Physics"), 0);
  }

  private void testBasicProperties(PartitionFunction partitionFunction, String functionName, int numPartitions) {
    testBasicProperties(partitionFunction, functionName, numPartitions, null);
  }

  private void testBasicProperties(PartitionFunction partitionFunction, String functionName, int numPartitions,
      Map<String, String> functionConfig) {
    assertEquals(partitionFunction.getName().toLowerCase(), functionName.toLowerCase());
    assertEquals(partitionFunction.getNumPartitions(), numPartitions);

    JsonNode jsonNode = JsonUtils.objectToJsonNode(partitionFunction);
    assertEquals(jsonNode.size(), 3);
    assertEquals(jsonNode.get("name").asText().toLowerCase(), functionName.toLowerCase());
    assertEquals(jsonNode.get("numPartitions").asInt(), numPartitions);

    JsonNode functionConfigNode = jsonNode.get("functionConfig");
    if (functionConfig == null) {
      assertTrue(functionConfigNode.isNull());
    } else {
      functionConfigNode.fields().forEachRemaining(nodeEntry -> {
        assertTrue(functionConfig.containsKey(nodeEntry.getKey()));
        assertEquals(nodeEntry.getValue().asText(), functionConfig.get(nodeEntry.getKey()));
      });
    }
  }

  /**
   * Tests the equivalence of org.apache.kafka.common.utils.Utils::murmur2 and
   * {@link MurmurPartitionFunction#getPartition}
   * Our implementation of murmur2 has been copied over from Utils::murmur2
   */
  @Test
  public void testMurmurEquivalence() {

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.apache.kafka.common.utils.Utils::murmur2 to those values and stored in expectedMurmurValues
    int[] expectedMurmurValues = new int[]{
        -1044832774, -594851693, 1441878663, 1766739604, 1034724141, -296671913, 443511156, 1483601453, 1819695080,
        -931669296
    };

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
      assertEquals(actualMurmurValue, expectedMurmurValue);
    }
  }

  /**
   * Tests the equivalence of partitioning using org.apache.kafka.common.utils.Utils::partition and
   * {@link MurmurPartitionFunction#getPartition}
   */
  @Test
  public void testMurmurPartitionFunctionEquivalence() {

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied {@link MurmurPartitionFunction} initialized with 5 partitions, by overriding
    // {@MurmurPartitionFunction::murmur2} with org
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
   * Tests the equivalence of kafka.producer.ByteArrayPartitioner::partition and {@link ByteArrayPartitionFunction
   * ::getPartition}
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
      String nextString = new String(array, UTF_8);
      int actualPartition = partitionFunction.getPartition(nextString);
      assertEquals(actualPartition, expectedPartition);
    }
  }
}
