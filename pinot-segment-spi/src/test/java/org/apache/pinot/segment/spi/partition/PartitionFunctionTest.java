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
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.hash.FnvHashFunctions;
import org.apache.pinot.spi.utils.hash.MurmurHashFunctions;
import org.testng.annotations.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


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
        assertEquals(partitionFunction.getPartition(Integer.toString(value)), expectedPartition);
      }

      // Test long values
      for (int j = 0; j < NUM_ROUNDS; j++) {
        long value = j == 0 ? Long.MIN_VALUE : random.nextLong();
        int expectedPartition = (int) (value % numPartitions);
        if (expectedPartition < 0) {
          expectedPartition += numPartitions;
        }
        assertEquals(partitionFunction.getPartition(Long.toString(value)), expectedPartition);
      }
    }
  }

  /**
   * Unit test for {@link MurmurPartitionFunction}.
   * <ul>
   *   <li> Tests that partition values are in expected range. </li>
   * </ul>
   */
  @Test
  public void testMurmurPartitioner() {
    // Both Murmur and Murmur2 are aliases for MurmurPartitionFunction
    testMurmurPartitioner("mUrmur");
    testMurmurPartitioner("mUrMuR2");
  }

  private void testMurmurPartitioner(String functionName) {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;

      PartitionFunction partitionFunction =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);

      testBasicProperties(partitionFunction, "murmur", numPartitions);

      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();
        testPartitionInExpectedRange(partitionFunction, value, numPartitions);
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
        testPartitionInExpectedRange(partitionFunction1, value, numPartitions);

        // check for the partition function with non-null functionConfig but without seed value.
        testPartitionInExpectedRange(partitionFunction2, value, numPartitions);

        // check for the partition function with non-null functionConfig and with seed value.
        testPartitionInExpectedRange(partitionFunction3, value, numPartitions);

        // check for the partition function with non-null functionConfig and with seed value and variant.
        testPartitionInExpectedRange(partitionFunction4, value, numPartitions);

        // check for the partition function with non-null functionConfig and with explicitly provided default seed
        // value and variant.
        testPartitionInExpectedRange(partitionFunction5, value, numPartitions);

        // check for the partition function with non-null functionConfig and with empty seed value and default variant.
        testPartitionInExpectedRange(partitionFunction6, value, numPartitions);

        // check for the partition function with non-null functionConfig and with empty seed value and empty variant.
        testPartitionInExpectedRange(partitionFunction7, value, numPartitions);
      }
    }
  }

  /**
   * Unit test for {@link FnvPartitionFunction}.
   * <ul>
   *   <li> Tests that default and explicit-default configs behave identically. </li>
   *   <li> Tests that all supported variants return partitions in the expected range. </li>
   * </ul>
   */
  @Test
  public void testFnvPartitioner() {
    long seed = System.currentTimeMillis();
    Random random = new Random(seed);

    for (int i = 0; i < NUM_ROUNDS; i++) {
      int numPartitions = random.nextInt(MAX_NUM_PARTITIONS) + 1;
      String functionName = "FnV";
      String valueTobeHashed = String.valueOf(random.nextInt());

      PartitionFunction partitionFunction1 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, null);
      int partitionNumWithNullConfig = partitionFunction1.getPartition(valueTobeHashed);

      Map<String, String> emptyFunctionConfig = new HashMap<>();
      PartitionFunction partitionFunction2 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, emptyFunctionConfig);
      assertEquals(partitionFunction2.getPartition(valueTobeHashed), partitionNumWithNullConfig);

      Map<String, String> defaultVariantConfig = new HashMap<>();
      defaultVariantConfig.put("variant", "fnv1a_32");
      PartitionFunction partitionFunction3 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, defaultVariantConfig);
      assertEquals(partitionFunction3.getPartition(valueTobeHashed), partitionNumWithNullConfig);

      Map<String, String> fnv132Config = new HashMap<>();
      fnv132Config.put("variant", "fnv1_32");
      PartitionFunction partitionFunction4 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, fnv132Config);

      Map<String, String> fnv164Config = new HashMap<>();
      fnv164Config.put("variant", "fnv1_64");
      PartitionFunction partitionFunction5 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, fnv164Config);

      Map<String, String> fnv1a64Config = new HashMap<>();
      fnv1a64Config.put("variant", "fnv1a_64");
      PartitionFunction partitionFunction6 =
          PartitionFunctionFactory.getPartitionFunction(functionName, numPartitions, fnv1a64Config);

      testBasicProperties(partitionFunction1, functionName, numPartitions);
      testBasicProperties(partitionFunction2, functionName, numPartitions, emptyFunctionConfig);
      testBasicProperties(partitionFunction3, functionName, numPartitions, defaultVariantConfig);
      testBasicProperties(partitionFunction4, functionName, numPartitions, fnv132Config);
      testBasicProperties(partitionFunction5, functionName, numPartitions, fnv164Config);
      testBasicProperties(partitionFunction6, functionName, numPartitions, fnv1a64Config);

      for (int j = 0; j < NUM_ROUNDS; j++) {
        int value = j == 0 ? Integer.MIN_VALUE : random.nextInt();
        testPartitionInExpectedRange(partitionFunction1, value, numPartitions);
        testPartitionInExpectedRange(partitionFunction2, value, numPartitions);
        testPartitionInExpectedRange(partitionFunction3, value, numPartitions);
        testPartitionInExpectedRange(partitionFunction4, value, numPartitions);
        testPartitionInExpectedRange(partitionFunction5, value, numPartitions);
        testPartitionInExpectedRange(partitionFunction6, value, numPartitions);
      }
    }
  }

  @Test
  public void testMurmur3Equivalence() {

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_64_String with seed = 0, applied right shift
    // on 32 bits to those values and stored in expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedMurmurValuesFor32BitX64WithZeroSeed = new int[]{
        -930531654, 1010637996, -1251084035, -1551293561, 1591443335, 181872103, 1308755538, -432310401, -701537488,
        674867586
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_64_String with seed = 0, applied right shift
    // on 32 bits those values and stored in expectedMurmurValuesFor32BitX64WithNonZeroSeed.
    int[] expectedMurmurValuesFor32BitX64WithNonZeroSeed = new int[]{
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

    // Test for 64 bit murmur3 hash with x64_64 variant and seed = 0 for String.
    testMurmur3Hash(0, expectedMurmurValuesFor32BitX64WithZeroSeed, true);

    // Test for 64 bit murmur3 hash with x64_64 variant and seed = 9001 for String.
    testMurmur3Hash(9001, expectedMurmurValuesFor32BitX64WithNonZeroSeed, true);

    // Test for 32 bit murmur3 hash with x86_32 variant and seed = 0 for byte array.
    testMurmur3Hash(0, expectedMurmurValuesFor32BitX86WithZeroSeed, false);

    // Test for 32 bit murmur3 hash with x86_32 variant and seed = 9001 for byte array.
    testMurmur3Hash(9001, expectedMurmurValuesFor32BitX86WithNonZeroSeed, false);
  }

  /**
   * Unit test for {@link MurmurPartitionFunction}.
   * <ul>
   *   <li> Tests that partition values are in expected range. </li>
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
        testPartitionInExpectedRange(partitionFunction, value, numPartitions);
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
        assertEquals(partitionFunction.getPartition(Integer.toString(value)), expectedPartition);
      }

      // Test double values
      for (int j = 0; j < NUM_ROUNDS; j++) {
        double value = j == 0 ? Double.NEGATIVE_INFINITY : random.nextDouble();
        int hashCode = Double.toString(value).hashCode();
        int expectedPartition = ((hashCode == Integer.MIN_VALUE) ? 0 : Math.abs(hashCode)) % numPartitions;
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
      functionConfigNode.properties().forEach(nodeEntry -> {
        assertTrue(functionConfig.containsKey(nodeEntry.getKey()));
        assertEquals(nodeEntry.getValue().asText(), functionConfig.get(nodeEntry.getKey()));
      });
    }
  }

  /**
   * Tests the equivalence of org.apache.kafka.common.utils.Utils::murmurHash2 and
   * {@link MurmurPartitionFunction#getPartition}
   * Our implementation of murmurHash2 has been copied over from Utils::murmurHash2
   */
  @Test
  public void testMurmurEquivalence() {

    // 10 values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.apache.kafka.common.utils.Utils::murmurHash2 to those values and stored in expectedMurmurValues
    int[] expectedMurmurValues = new int[]{
        -1044832774, -594851693, 1441878663, 1766739604, 1034724141, -296671913, 443511156, 1483601453, 1819695080,
        -931669296
    };

    long seed = 100;
    Random random = new Random(seed);

    // Generate the same values as above - 10 random values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply {@link MurmurPartitionFunction::murmurHash2
    // compare with stored results
    byte[] bytes = new byte[7];
    for (int expectedMurmurValue : expectedMurmurValues) {
      random.nextBytes(bytes);
      assertEquals(MurmurHashFunctions.murmurHash2(bytes), expectedMurmurValue);
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
    // {@MurmurPartitionFunction::murmurHash2} with org
    // .apache.kafka.common.utils.Utils::murmurHash2
    // stored the results in expectedPartitions
    int[] expectedPartitions = new int[]{1, 4, 4, 1, 1, 2, 0, 4, 2, 3};

    // initialized {@link MurmurPartitionFunction} with 5 partitions
    int numPartitions = 5;
    MurmurPartitionFunction murmurPartitionFunction = new MurmurPartitionFunction(numPartitions);

    // generate the same 10 String values
    // Apply the partition function and compare with stored results
    testPartitionFunction(murmurPartitionFunction, expectedPartitions);
  }

  @Test
  public void testMurmurPartitionFunctionUseRawBytes() {
    int numPartitions = 5;
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("useRawBytes", "true");
    MurmurPartitionFunction partitionFunction = new MurmurPartitionFunction(numPartitions, functionConfig);

    // When useRawBytes is true, the hex-encoded value should be decoded back to raw bytes before hashing.
    // This means getPartition(hexString) should produce the same result as hashing the original raw bytes.
    byte[] rawBytes = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
    String hexValue = BytesUtils.toHexString(rawBytes);

    int expectedPartition =
        (MurmurHashFunctions.murmurHash2(rawBytes) & Integer.MAX_VALUE) % numPartitions;
    assertEquals(partitionFunction.getPartition(hexValue), expectedPartition);

    // Without useRawBytes, the same hex string should be treated as UTF-8 text (different result).
    MurmurPartitionFunction defaultPartitionFunction = new MurmurPartitionFunction(numPartitions, null);
    int defaultPartition =
        (MurmurHashFunctions.murmurHash2(hexValue.getBytes(UTF_8)) & Integer.MAX_VALUE) % numPartitions;
    assertEquals(defaultPartitionFunction.getPartition(hexValue), defaultPartition);
  }

  @Test
  public void testMurmur3PartitionFunctionUseRawBytes() {
    int numPartitions = 5;
    byte[] rawBytes = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
    String hexValue = BytesUtils.toHexString(rawBytes);

    // Test x86_32 variant with useRawBytes
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("useRawBytes", "true");
    Murmur3PartitionFunction partitionFunction = new Murmur3PartitionFunction(numPartitions, functionConfig);

    int expectedPartition =
        (MurmurHashFunctions.murmurHash3X86Bit32(rawBytes, 0) & Integer.MAX_VALUE) % numPartitions;
    assertEquals(partitionFunction.getPartition(hexValue), expectedPartition);

    // Test x64_32 variant with useRawBytes
    functionConfig.put("variant", "x64_32");
    Murmur3PartitionFunction partitionFunctionX64 = new Murmur3PartitionFunction(numPartitions, functionConfig);

    int expectedPartitionX64 =
        (MurmurHashFunctions.murmurHash3X64Bit32(rawBytes, 0) & Integer.MAX_VALUE) % numPartitions;
    assertEquals(partitionFunctionX64.getPartition(hexValue), expectedPartitionX64);

    // Test x86_32 variant with useRawBytes and non-zero seed
    functionConfig.remove("variant");
    functionConfig.put("seed", "9001");
    Murmur3PartitionFunction partitionFunctionWithSeed = new Murmur3PartitionFunction(numPartitions, functionConfig);

    int expectedPartitionWithSeed =
        (MurmurHashFunctions.murmurHash3X86Bit32(rawBytes, 9001) & Integer.MAX_VALUE) % numPartitions;
    assertEquals(partitionFunctionWithSeed.getPartition(hexValue), expectedPartitionWithSeed);

    // Without useRawBytes, the same hex string should be treated as UTF-8 text.
    Murmur3PartitionFunction defaultPartitionFunction = new Murmur3PartitionFunction(numPartitions, null);
    int defaultPartition =
        (MurmurHashFunctions.murmurHash3X86Bit32(hexValue.getBytes(UTF_8), 0) & Integer.MAX_VALUE) % numPartitions;
    assertEquals(defaultPartitionFunction.getPartition(hexValue), defaultPartition);
  }

  @Test
  public void testFnvPartitionFunctionUseRawBytes() {
    int numPartitions = 5;
    byte[] rawBytes = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
    String hexValue = BytesUtils.toHexString(rawBytes);

    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("useRawBytes", "true");
    FnvPartitionFunction defaultPartitionFunction = new FnvPartitionFunction(numPartitions, functionConfig);

    int expectedPartition = getMaskPartition(FnvHashFunctions.fnv1aHash32(rawBytes), numPartitions);
    assertEquals(defaultPartitionFunction.getPartition(hexValue), expectedPartition);

    functionConfig.put("variant", "fnv1_32");
    FnvPartitionFunction fnv132PartitionFunction = new FnvPartitionFunction(numPartitions, functionConfig);
    int expectedPartitionFnv132 = getMaskPartition(FnvHashFunctions.fnv1Hash32(rawBytes), numPartitions);
    assertEquals(fnv132PartitionFunction.getPartition(hexValue), expectedPartitionFnv132);

    functionConfig.put("variant", "fnv1_64");
    FnvPartitionFunction fnv164PartitionFunction = new FnvPartitionFunction(numPartitions, functionConfig);
    int expectedPartitionFnv164 = getMaskPartition(FnvHashFunctions.fnv1Hash64(rawBytes), numPartitions);
    assertEquals(fnv164PartitionFunction.getPartition(hexValue), expectedPartitionFnv164);

    functionConfig.put("variant", "fnv1a_64");
    FnvPartitionFunction fnv1a64PartitionFunction = new FnvPartitionFunction(numPartitions, functionConfig);
    int expectedPartitionFnv1a64 = getMaskPartition(FnvHashFunctions.fnv1aHash64(rawBytes), numPartitions);
    assertEquals(fnv1a64PartitionFunction.getPartition(hexValue), expectedPartitionFnv1a64);

    FnvPartitionFunction utf8PartitionFunction = new FnvPartitionFunction(numPartitions, null);
    int expectedUtf8Partition = getMaskPartition(FnvHashFunctions.fnv1aHash32(hexValue.getBytes(UTF_8)),
        numPartitions);
    assertEquals(utf8PartitionFunction.getPartition(hexValue), expectedUtf8Partition);
  }

  @Test
  public void testFnvPartitionFunctionNegativePartitionHandlingConfig() {
    int numPartitions = 16;
    String value = "";
    int hash = FnvHashFunctions.fnv1aHash32(value.getBytes(UTF_8));
    int saramaCompatPartition = getSaramaCompatPartition(hash, numPartitions);
    int javaCompatiblePartition = (hash & Integer.MAX_VALUE) % numPartitions;

    assertNotEquals(saramaCompatPartition, javaCompatiblePartition);

    assertEquals(new FnvPartitionFunction(numPartitions, null).getPartition(value), javaCompatiblePartition);

    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("negativePartitionHandling", "abs");
    assertEquals(new FnvPartitionFunction(numPartitions, functionConfig).getPartition(value), saramaCompatPartition);
  }

  @Test
  public void testFnvPartitionFunctionNegativePartitionHandlingFor64BitVariants() {
    int numPartitions = 16;
    String value = "";
    long hash = FnvHashFunctions.fnv1aHash64(value.getBytes(UTF_8));
    int saramaCompatPartition = getSaramaCompatPartition(hash, numPartitions);
    int javaCompatiblePartition = getMaskPartition(hash, numPartitions);

    assertNotEquals(saramaCompatPartition, javaCompatiblePartition);

    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("variant", "fnv1a_64");
    assertEquals(new FnvPartitionFunction(numPartitions, functionConfig).getPartition(value), javaCompatiblePartition);

    functionConfig.put("negativePartitionHandling", "abs");
    assertEquals(new FnvPartitionFunction(numPartitions, functionConfig).getPartition(value), saramaCompatPartition);
  }

  @Test
  public void testFnvPartitionFunctionRejectsInvalidNegativePartitionHandling() {
    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("negativePartitionHandling", "saramaCompat");

    IllegalArgumentException exception =
        expectThrows(IllegalArgumentException.class, () -> new FnvPartitionFunction(4, functionConfig));
    assertEquals(exception.getMessage(),
        "FNV negative partition handling must be mask or abs, but was: 'saramaCompat'");
  }

  @Test
  public void testUseRawBytesThroughFactory() {
    int numPartitions = 5;
    byte[] rawBytes = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
    String hexValue = BytesUtils.toHexString(rawBytes);
    int expectedPartition =
        (MurmurHashFunctions.murmurHash2(rawBytes) & Integer.MAX_VALUE) % numPartitions;

    Map<String, String> functionConfig = new HashMap<>();
    functionConfig.put("useRawBytes", "true");

    // Test Murmur alias through factory
    PartitionFunction murmurFn =
        PartitionFunctionFactory.getPartitionFunction("Murmur", numPartitions, functionConfig);
    assertEquals(murmurFn.getPartition(hexValue), expectedPartition);
    assertEquals(murmurFn.getFunctionConfig().get("useRawBytes"), "true");

    // Test Murmur2 alias through factory
    PartitionFunction murmur2Fn =
        PartitionFunctionFactory.getPartitionFunction("Murmur2", numPartitions, functionConfig);
    assertEquals(murmur2Fn.getPartition(hexValue), expectedPartition);
    assertEquals(murmur2Fn.getFunctionConfig().get("useRawBytes"), "true");

    // Test Murmur3 through factory
    int expectedPartitionMurmur3 =
        (MurmurHashFunctions.murmurHash3X86Bit32(rawBytes, 0) & Integer.MAX_VALUE) % numPartitions;
    PartitionFunction murmur3Fn =
        PartitionFunctionFactory.getPartitionFunction("Murmur3", numPartitions, functionConfig);
    assertEquals(murmur3Fn.getPartition(hexValue), expectedPartitionMurmur3);
    assertEquals(murmur3Fn.getFunctionConfig().get("useRawBytes"), "true");

    // Test FNV through factory with the default fnv1a_32 variant
    int expectedPartitionFnv = getMaskPartition(FnvHashFunctions.fnv1aHash32(rawBytes), numPartitions);
    PartitionFunction fnvFn =
        PartitionFunctionFactory.getPartitionFunction("FNV", numPartitions, functionConfig);
    assertEquals(fnvFn.getPartition(hexValue), expectedPartitionFnv);
    assertEquals(fnvFn.getFunctionConfig().get("useRawBytes"), "true");
  }

  @Test
  public void testMurmur3PartitionFunctionEquivalence() {

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 0 to those values and stored in
    // expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedPartitions32BitsX64WithZeroSeed = new int[]{
        4, 1, 3, 2, 0, 3, 3, 2, 0, 1
    };

    // 10 String values of size 7, were randomly generated, using {@link Random::nextBytes} with seed 100
    // Applied org.infinispan.commons.hash.MurmurHash3::MurmurHash3_x64_32 with seed = 9001 to those values and
    // stored in expectedMurmurValuesFor32BitX64WithZeroSeed.
    int[] expectedPartitions32BitsX64WithNonZeroSeed = new int[]{
        2, 1, 4, 2, 2, 2, 2, 1, 3, 3
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
    testPartitionFunction(murmur3PartitionFunction1, expectedPartitions32BitsX64WithZeroSeed);
    testPartitionFunction(murmur3PartitionFunction2, expectedPartitions32BitsX64WithNonZeroSeed);

    // Generate the same 10 String values. Test if the calculated values are equal to the expected values for the x86_32
    // variant with seed = 0 and x86_32 variant with seed = 9001.
    testPartitionFunction(murmur3PartitionFunction3, expectedPartitions32BitsX86WithZeroSeed);
    testPartitionFunction(murmur3PartitionFunction4, expectedPartitions32BitsX86WithNonZeroSeed);
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
    testPartitionFunction(byteArrayPartitionFunction, expectedPartitions);
  }

  private void testPartitionInExpectedRange(PartitionFunction partitionFunction, Object value, int numPartitions) {
    int partition = partitionFunction.getPartition(value.toString());
    assertTrue(partition >= 0 && partition < numPartitions);
  }

  private void testPartitionFunction(PartitionFunction partitionFunction, int[] expectedPartitions) {
    long seed = 100;
    Random random = new Random(seed);

    // Generate 10 random String values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply given partition function
    // compare with expectedPartitions
    byte[] bytes = new byte[7];
    for (int expectedPartitionNumber : expectedPartitions) {
      random.nextBytes(bytes);
      String nextString = new String(bytes, UTF_8);
      assertEquals(partitionFunction.getPartition(nextString), expectedPartitionNumber);
    }
  }

  private void testMurmur3Hash(int hashSeed, int[] expectedHashValues, boolean useX64) {
    long seed = 100;
    Random random = new Random(seed);

    // Generate 10 random String values of size 7, using {@link Random::nextBytes} with seed 100
    // Apply given partition function
    // compare with expectedPartitions
    byte[] bytes = new byte[7];
    for (int expectedHashValue : expectedHashValues) {
      random.nextBytes(bytes);
      String nextString = new String(bytes, UTF_8);
      int actualHashValue = useX64 ? MurmurHashFunctions.murmurHash3X64Bit32(nextString, hashSeed)
          : MurmurHashFunctions.murmurHash3X86Bit32(bytes, hashSeed);
      assertEquals(actualHashValue, expectedHashValue);
    }
  }

  private int getSaramaCompatPartition(int hash, int numPartitions) {
    int partition = hash % numPartitions;
    return partition < 0 ? -partition : partition;
  }

  private int getSaramaCompatPartition(long hash, int numPartitions) {
    long partition = hash % numPartitions;
    return (int) (partition < 0 ? -partition : partition);
  }

  private int getMaskPartition(int hash, int numPartitions) {
    return (hash & Integer.MAX_VALUE) % numPartitions;
  }

  private int getMaskPartition(long hash, int numPartitions) {
    return (int) ((hash & Long.MAX_VALUE) % numPartitions);
  }
}
