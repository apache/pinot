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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
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

  @Test
  public void testPartioningOnByteArray() {
    byte[] arr1 = new byte[] {5, 1, 9, 2, 6};
    byte[] arr2 = new byte[] {5, 1, 9, 2, 6};
    byte[] arr3 = new byte[] {10, 1, 9, 2, 6};
    String functionName = "mUrmur";
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(functionName, 5, null);
    Assert.assertTrue(partitionFunction.getValueToPartition(arr1) == partitionFunction.getValueToPartition(arr2));
    Assert.assertTrue(partitionFunction.getValueToPartition(arr1) != partitionFunction.getValueToPartition(arr3));
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
   *   <li> Tests the default behaviors when functionConfig is not provided or only one of the optional parameters of
   *   functionConfig is provided.</li>
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

      // Put variant value as "x86_32" in function config.
      functionConfig.put("variant", "x86_32");

      // Put seed value as 0 in function config.
      functionConfig.put("seed", "0");

      // Create partition function with function config present with variant provided as "x86_32" in function config.
      PartitionFunction partitionFunction5 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, functionConfig);

      // Partition number should be equal as partitionNumWithNullConfig and partitionNumWithNoSeedValue as this is
      // default behavior.
      assertEquals(partitionFunction5.getPartition(valueTobeHashed), partitionNumWithNullConfig);

      // Replace seed value as empty string in function config.
      functionConfig.put("seed", "");

      // Create partition function with function config present with variant provided as "x86_32" and empty seed
      // value in functionConfig.
      PartitionFunction partitionFunction6 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, functionConfig);

      // Partition number should be equal as partitionNumWithNullConfig and partitionNumWithNoSeedValue as this is
      // default behavior.
      assertEquals(partitionFunction6.getPartition(valueTobeHashed), partitionNumWithNullConfig);

      // Replace variant value as empty string in function config.
      functionConfig.put("variant", "");

      // Create partition function with function config present with empty variant and empty seed.
      PartitionFunction partitionFunction7 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, functionConfig);

      // Partition number should be equal as partitionNumWithNullConfig and partitionNumWithNoSeedValue as this is
      // default behavior.
      assertEquals(partitionFunction7.getPartition(valueTobeHashed), partitionNumWithNullConfig);

      testBasicProperties(partitionFunction1, functionName, numPartitions);
      testBasicProperties(partitionFunction2, functionName, numPartitions, functionConfig);
      testBasicProperties(partitionFunction3, functionName, numPartitions, functionConfig);
      testBasicProperties(partitionFunction4, functionName, numPartitions, functionConfig);
      testBasicProperties(partitionFunction5, functionName, numPartitions, functionConfig);
      testBasicProperties(partitionFunction6, functionName, numPartitions, functionConfig);
      testBasicProperties(partitionFunction7, functionName, numPartitions, functionConfig);

      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();

        // check for the partition function with functionConfig as null.
        testToStringAndPartitionNumber(partitionFunction1, value, numPartitions);

        // check for the partition function with non-null functionConfig but without seed value.
        testToStringAndPartitionNumber(partitionFunction2, value, numPartitions);

        // check for the partition function with non-null functionConfig and with seed value.
        testToStringAndPartitionNumber(partitionFunction3, value, numPartitions);

        // check for the partition function with non-null functionConfig and with seed value and variant.
        testToStringAndPartitionNumber(partitionFunction4, value, numPartitions);

        // check for the partition function with non-null functionConfig and with explicitly provided default seed
        // value and variant.
        testToStringAndPartitionNumber(partitionFunction5, value, numPartitions);

        // check for the partition function with non-null functionConfig and with empty seed value and default variant.
        testToStringAndPartitionNumber(partitionFunction6, value, numPartitions);

        // check for the partition function with non-null functionConfig and with empty seed value and empty variant.
        testToStringAndPartitionNumber(partitionFunction7, value, numPartitions);
      }
    }
  }

  @Test
  public void testMurmur3Equivalence() {

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeedForByteArray.
    int[] expectedMurmurValuesFor32BitX64WithZeroSeedForByteArray = new int[]{
        -1569442405, -921191038, 16439113, -881572510, 2111401876, 655879980, 1409856380, -1348364123, -1770645361,
        1277101955
    };

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 9001 to those values and
    // stored in expectedMurmurValuesFor32BitX64WithNonZeroSeedForByteArray.
    int[] expectedMurmurValuesFor32BitX64WithNonZeroSeedForByteArray = new int[]{
        698083240, 174075836, -938825597, 155806634, -831733828, 319389887, -939822329, -1785781936, -1796939240,
        757512622
    };

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeedForLongArray.
    int[] expectedMurmurValuesFor32BitX64WithZeroSeedForLongArray = new int[]{
        -621156783, -1341356662, 1615513844, 1608247599, -1339558745, -1782606270, 1204009437, 8939246, -42073819,
        1268621125
    };

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 9001 to those values and
    // stored in expectedMurmurValuesFor32BitX64WithNonZeroSeedForLongArray.
    int[] expectedMurmurValuesFor32BitX64WithNonZeroSeedForLongArray = new int[]{
        -159780599, 1266925141, -2039451704, 237323842, -1373894107, -1718192521, 314068498, 1377198162, 1239340429,
        -1643307044
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_64_String with seed = 0, applied right shift
    // on 32 bits to those values and stored in expectedMurmurValuesFor32BitX64WithZeroSeedForString.
    int[] expectedMurmurValuesFor32BitX64WithZeroSeedForString = new int[]{
        -930531654, 1010637996, -1251084035, -1551293561, 1591443335, 181872103, 1308755538, -432310401, -701537488,
        674867586
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_64_String with seed = 0, applied right shift
    // on 32 bits those values and stored in expectedMurmurValuesFor32BitX64WithNonZeroSeedForString.
    int[] expectedMurmurValuesFor32BitX64WithNonZeroSeedForString = new int[]{
        1558697417, 933581816, 1071120824, 1964512897, 1629803052, 2037246152, -1867319466, -1003065762, -275998120,
        1386652858
    };

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x86_32 with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX86WithZeroSeed.
    int[] expectedMurmurValuesFor32BitX86WithZeroSeed = new int[]{
        1255034832, -395463542, 659973067, 1070436837, -1193041642, -1412829846, -483463488, -1385092001, 568671606,
        -807299446
    };

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x86_32 with seed = 9001 to those values and
    // stored in expectedMurmurValuesFor32BitX86WithNonZeroSeed.
    int[] expectedMurmurValuesFor32BitX86WithNonZeroSeed = new int[]{
        -590969347, -315366997, 1642137565, -1732240651, -597560989, -1430018124, -448506674, 410998174, -1912106487,
        -19253806
    };

    // Test for 32 bit murmur3 hash with x64_64 variant and seed = 0 for byte array.
    testMurmur3HashEquivalenceForDifferentDataTypes(0, expectedMurmurValuesFor32BitX64WithZeroSeedForByteArray,
        "byteArray", "x64_32");

    // Test for 32 bit murmur3 hash with x64_64 variant and seed = 9001 for byte array.
    testMurmur3HashEquivalenceForDifferentDataTypes(9001, expectedMurmurValuesFor32BitX64WithNonZeroSeedForByteArray,
        "byteArray", "x64_32");

    // Test for 32 bit murmur3 hash with x64_64 variant and seed = 0 for long array.
    testMurmur3HashEquivalenceForDifferentDataTypes(0, expectedMurmurValuesFor32BitX64WithZeroSeedForLongArray,
        "longArray", "x64_32");

    // Test for 32 bit murmur3 hash with x64_64 variant and seed = 9001 for long array.
    testMurmur3HashEquivalenceForDifferentDataTypes(9001, expectedMurmurValuesFor32BitX64WithNonZeroSeedForLongArray,
        "longArray", "x64_32");

    // Test for 64 bit murmur3 hash with x64_64 variant and seed = 0 for String.
    testMurmur3HashEquivalenceForDifferentDataTypes(0, expectedMurmurValuesFor32BitX64WithZeroSeedForString, "String",
        "x64_32");

    // Test for 64 bit murmur3 hash with x64_64 variant and seed = 9001 for String.
    testMurmur3HashEquivalenceForDifferentDataTypes(9001, expectedMurmurValuesFor32BitX64WithNonZeroSeedForString,
        "String", "x64_32");

    // Test for 32 bit murmur3 hash with x86_32 variant and seed = 0 for byte array.
    testMurmur3HashEquivalenceForDifferentDataTypes(0, expectedMurmurValuesFor32BitX86WithZeroSeed, "byteArray",
        "x86_32");

    // Test for 32 bit murmur3 hash with x86_32 variant and seed = 9001 for byte array.
    testMurmur3HashEquivalenceForDifferentDataTypes(9001, expectedMurmurValuesFor32BitX86WithNonZeroSeed, "byteArray",
        "x86_32");
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

  @Test
  public void testMurmur3PartitionFunctionEquivalence() {

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedPartitions32BitsX64WithZeroSeedForByteArrayAndString = new int[]{
        4, 1, 3, 2, 0, 3, 3, 2, 0, 1
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 9001 to those values and
    // stored in expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedPartitions32BitsX64WithNonZeroSeedForByteArrayAndString = new int[]{
        2, 1, 4, 2, 2, 2, 2, 1, 3, 3
    };

    // 10 long[] values of size 10, were randomly generated, using {@link Random::nextLong} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedPartitions32BitsX64WithZeroSeedForLongArray.
    int[] expectedPartitions32BitsX64WithZeroSeedForLongArray = new int[]{
        0, 1, 4, 4, 3, 3, 2, 1, 4, 0
    };

    // 10 long[] values of size 10, were randomly generated, using {@link Random::nextLong} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedPartitions32BitsX64WithNonZeroSeedForLongArray.
    int[] expectedPartitions32BitsX64WithNonZeroSeedForLongArray = new int[]{
        4, 1, 4, 2, 1, 2, 3, 2, 4, 4
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied com.google.common.hash.hashing::murmur3_32_fixed with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedPartitions32BitsX86WithZeroSeed = new int[]{
        4, 3, 3, 2, 3, 4, 0, 3, 1, 4
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied com.google.common.hash.hashing::murmur3_32_fixed with seed = 9001 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedPartitions32BitsX86WithNonZeroSeed = new int[]{
        2, 1, 3, 2, 2, 1, 1, 4, 4, 2
    };

    // initialized {@link Murmur3PartitionFunction} with 5 partitions and variant as "x64_32".
    int numPartitions = 5;
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("variant", "x64_32");

    // x64_32 variant with seed = 0.
    Murmur3PartitionFunction murmur3PartitionFunction1 = new Murmur3PartitionFunction(numPartitions, functionConfig);

    // Put seed value in "seed" field in the function config.
    functionConfig.put("seed", Integer.toString(9001));

    // x64_32 variant with seed = 9001.
    Murmur3PartitionFunction murmur3PartitionFunction2 = new Murmur3PartitionFunction(numPartitions, functionConfig);

    // x86_32 variant with seed = 0.
    Murmur3PartitionFunction murmur3PartitionFunction3 = new Murmur3PartitionFunction(numPartitions, null);

    // Remove the variant field.
    functionConfig.remove("variant");

    // x86_32 bit variant with seed = 9001.
    Murmur3PartitionFunction murmur3PartitionFunction4 = new Murmur3PartitionFunction(numPartitions, functionConfig);

    // Generate the same 10 String values. Test if the calculated values are equal for both String and byte[] (they
    // should be equal when String is converted to byte[]) and if the values are equal to the expected values for the
    // x64_32 variant with seed = 0 and x64_32 variant with seed = 9001.
    testPartitionFunctionEquivalenceWithStringAndByteArray(murmur3PartitionFunction1,
        expectedPartitions32BitsX64WithZeroSeedForByteArrayAndString);
    testPartitionFunctionEquivalenceWithStringAndByteArray(murmur3PartitionFunction2,
        expectedPartitions32BitsX64WithNonZeroSeedForByteArrayAndString);

    // Generate the same 10 long[] values. Test if the calculated values are equal to the expected values for the x64_32
    // variant with seed = 0 and x64_32 variant with seed = 9001.
    testPartitionFunctionEquivalenceWithLongArray(murmur3PartitionFunction1,
        expectedPartitions32BitsX64WithZeroSeedForLongArray);
    testPartitionFunctionEquivalenceWithLongArray(murmur3PartitionFunction2,
        expectedPartitions32BitsX64WithNonZeroSeedForLongArray);

    // Generate the same 10 String values. Test if the calculated values are equal to the expected values for the x86_32
    // variant with seed = 0 and x86_32 variant with seed = 9001.
    testPartitionFunctionEquivalence(murmur3PartitionFunction3, expectedPartitions32BitsX86WithZeroSeed);
    testPartitionFunctionEquivalence(murmur3PartitionFunction4, expectedPartitions32BitsX86WithNonZeroSeed);
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

  private void testPartitionFunctionEquivalenceWithStringAndByteArray(PartitionFunction partitionFunction,
      int[] expectedPartitions) {
    long seed = 100;
    Random random = new Random(seed);

    // Generate 10 random String values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply given partition function
    // compare with expectedPartitions
    byte[] array = new byte[7];
    for (int expectedPartitionNumber : expectedPartitions) {
      random.nextBytes(array);
      String nextString = new String(array, UTF_8);
      int actualPartitionNumberFromString = partitionFunction.getPartition(nextString);
      int actualPartitionNumberFromByteArray = partitionFunction.getPartition(nextString.getBytes(UTF_8));
      assertEquals(actualPartitionNumberFromString, actualPartitionNumberFromByteArray);
      assertEquals(actualPartitionNumberFromString, expectedPartitionNumber);
    }
  }

  private void testPartitionFunctionEquivalenceWithLongArray(PartitionFunction partitionFunction,
      int[] expectedPartitions) {
    int seed = 100;
    Random random = new Random(seed);

    // Create a list of 10 long[] values using ArrayList, each of size 10.
    List<long[]> longList = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      long[] longArray = new long[10];
      for (int j = 0; j < 10; j++) {
        longArray[j] = random.nextLong();
      }
      longList.add(longArray);
    }

    // Apply the partition function and compare with expected values.
    for (int i = 0; i < 10; i++) {
      int actualPartitionNumberFromLongArray = partitionFunction.getPartition(longList.get(i));
      assertEquals(actualPartitionNumberFromLongArray, expectedPartitions[i]);
    }
  }

  private void testMurmur3HashEquivalenceForDifferentDataTypes(int hashSeed, int[] expectedHashValues, String dataType,
      String variant) {
    long seed = 100;
    Random random;
    int numPartitions = 5;
    Murmur3PartitionFunction murmur3PartitionFunction = new Murmur3PartitionFunction(numPartitions, null);

    switch (dataType.toLowerCase()) {
      case "string":
        // Generate 10 random String values of size 7, using {@link Random::nextBytes} with seed 100
        // Apply given partition function
        // compare with expectedPartitions
        random = new Random(seed);
        byte[] array1 = new byte[7];
        for (int expectedHashValue : expectedHashValues) {
          random.nextBytes(array1);
          String nextString = new String(array1, UTF_8);
          int actualHashValueFromString = murmur3PartitionFunction.murmur3Hash32BitsX64(nextString, hashSeed);
          assertEquals(actualHashValueFromString, expectedHashValue);
        }
        break;
      case "bytearray":
        // Generate 10 random String values of size 7, using {@link Random::nextBytes} with seed 100
        // Apply given partition function
        // compare with expectedPartitions
        random = new Random(seed);
        int actualHashValueFromByteArray;
        byte[] array2 = new byte[7];
        for (int expectedHashValue : expectedHashValues) {
          random.nextBytes(array2);
          if (variant.equals("x64_32")) {
            actualHashValueFromByteArray = murmur3PartitionFunction.murmur3Hash32BitsX64(array2, hashSeed);
          } else {
            actualHashValueFromByteArray = murmur3PartitionFunction.murmur3Hash32BitsX86(array2, hashSeed);
          }
          assertEquals(actualHashValueFromByteArray, expectedHashValue);
        }
        break;
      case "longarray":
        random = new Random(seed);
        // Create a list of 10 long[] values using ArrayList, each of size 10.
        List<long[]> longList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
          long[] longArray = new long[10];
          for (int j = 0; j < 10; j++) {
            longArray[j] = random.nextLong();
          }
          longList.add(longArray);
        }

        // Apply the partition function and compare with expected values.
        for (int i = 0; i < 10; i++) {
          int actualHashValueFromLongArray = murmur3PartitionFunction.murmur3Hash32BitsX64(longList.get(i), hashSeed);
          assertEquals(actualHashValueFromLongArray, expectedHashValues[i]);
        }
        break;
        default:
    }
  }
  private void testToStringAndPartitionNumber(PartitionFunction partitionFunction, int testValueForGetPartition,
      int numPartitions) {
    int partition1 = partitionFunction.getPartition(testValueForGetPartition);
    int partition2 = partitionFunction.getPartition(Integer.toString(testValueForGetPartition));
    assertEquals(partition1, partition2);
    assertTrue(partition1 >= 0 && partition1 < numPartitions);
  }
}
