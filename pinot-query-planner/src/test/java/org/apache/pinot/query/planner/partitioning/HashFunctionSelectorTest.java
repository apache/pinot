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
package org.apache.pinot.query.planner.partitioning;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test for {@link HashFunctionSelector}
 */
public class HashFunctionSelectorTest {

  @Test
  public void testAbsHashCode() {
    String value = "test";
    int hash1 = HashFunctionSelector.computeHash(value, "abshashcode");
    int hash2 = HashFunctionSelector.computeHash(value, "abshashcode");

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);
  }

  @Test
  public void testMurmur2() {
    String value = "test";
    int hash1 = HashFunctionSelector.computeHash(value, "murmur");
    int hash2 = HashFunctionSelector.computeHash(value, "murmur");

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);

    // Should be different from absHashCode
    int absHash = HashFunctionSelector.computeHash(value, "abshashcode");
    Assert.assertNotEquals(hash1, absHash);
  }

  @Test
  public void testMurmur3() {
    String value = "test";
    int hash1 = HashFunctionSelector.computeHash(value, "murmur3");
    int hash2 = HashFunctionSelector.computeHash(value, "murmur3");

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);

    // Should be different from other hash functions
    int absHash = HashFunctionSelector.computeHash(value, "abshashcode");
    int murmur2Hash = HashFunctionSelector.computeHash(value, "murmur2");
    Assert.assertNotEquals(hash1, absHash);
    Assert.assertNotEquals(hash1, murmur2Hash);
  }

  @Test
  public void testHashCode() {
    String value = "test";
    int hash1 = HashFunctionSelector.computeHash(value, "hashcode");
    int hash2 = HashFunctionSelector.computeHash(value, "hashcode");

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);

    // Should be different from murmur and murmur3 but same as absHashCode
    int absHash = HashFunctionSelector.computeHash(value, "abshashcode");
    int murmur2Hash = HashFunctionSelector.computeHash(value, "murmur");
    int murmur3Hash = HashFunctionSelector.computeHash(value, "murmur3");
    Assert.assertEquals(hash1, absHash);
    Assert.assertNotEquals(hash1, murmur2Hash);
    Assert.assertNotEquals(hash1, murmur3Hash);
  }

  @Test
  public void testNullValue() {
    // Null values should return 0 for all hash functions
    Assert.assertEquals(HashFunctionSelector.computeHash(null, "abshashcode"), 0);
    Assert.assertEquals(HashFunctionSelector.computeHash(null, "murmur"), 0);
    Assert.assertEquals(HashFunctionSelector.computeHash(null, "murmur3"), 0);
    Assert.assertEquals(HashFunctionSelector.computeHash(null, "cityhash"), 0);
  }

  @Test
  public void testUnknownHashFunction() {
    String value = "test";
    // Unknown hash function should default to absHashCode
    int hash = HashFunctionSelector.computeHash(value, "unknown");
    int expectedHash = HashFunctionSelector.computeHash(value, "abshashcode");
    Assert.assertEquals(hash, expectedHash);
  }

  @Test
  public void testCaseInsensitive() {
    String value = "test";
    int hash1 = HashFunctionSelector.computeHash(value, "MURMUR");
    int hash2 = HashFunctionSelector.computeHash(value, "murmur");
    Assert.assertEquals(hash1, hash2);
  }
}
