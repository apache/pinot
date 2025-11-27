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

  private final HashFunctionSelector.SvHasher _abshashcode = HashFunctionSelector.getSvHasher("abshashcode");
  private final HashFunctionSelector.SvHasher _murmur = HashFunctionSelector.getSvHasher("murmur");
  private final HashFunctionSelector.SvHasher _murmur2 = HashFunctionSelector.getSvHasher("murmur2");
  private final HashFunctionSelector.SvHasher _murmur3 = HashFunctionSelector.getSvHasher("murmur3");

  @Test
  public void testAbsHashCode() {
    String value = "test";
    int hash1 = _abshashcode.hash(value);
    int hash2 = _abshashcode.hash(value);

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);
  }

  @Test
  public void testMurmur2() {
    String value = "test";
    int hash1 = _murmur.hash(value);
    int hash2 = _murmur.hash(value);

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);

    // Should be different from absHashCode
    int absHash = _abshashcode.hash(value);
    Assert.assertNotEquals(hash1, absHash);
  }

  @Test
  public void testMurmur3() {
    String value = "test";

    int hash1 = _murmur3.hash(value);
    int hash2 = _murmur3.hash(value);

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);

    // Should be different from other hash functions
    int absHash = _abshashcode.hash(value);
    int murmur2Hash = _murmur2.hash(value);
    Assert.assertNotEquals(hash1, absHash);
    Assert.assertNotEquals(hash1, murmur2Hash);
  }

  @Test
  public void testHashCode() {
    String value = "test";
    HashFunctionSelector.SvHasher svHasher = HashFunctionSelector.getSvHasher("hashcode");
    int hash1 = svHasher.hash(value);
    int hash2 = svHasher.hash(value);

    // Same input should produce same hash
    Assert.assertEquals(hash1, hash2);

    // Should be positive
    Assert.assertTrue(hash1 >= 0);

    // Should be different from murmur and murmur3 but same as absHashCode
    int absHash = _abshashcode.hash(value);
    int murmur2Hash = _murmur.hash(value);
    int murmur3Hash = _murmur3.hash(value);
    Assert.assertEquals(hash1, absHash);
    Assert.assertNotEquals(hash1, murmur2Hash);
    Assert.assertNotEquals(hash1, murmur3Hash);
  }

  @Test
  public void testNullValue() {
    // Null values should return 0 for all hash functions
    Assert.assertEquals(_abshashcode.hash(null), 0);
    Assert.assertEquals(_murmur.hash(null), 0);
    Assert.assertEquals(_murmur3.hash(null), 0);
    HashFunctionSelector.SvHasher cityhash = HashFunctionSelector.getSvHasher("cityhash");
    Assert.assertEquals(cityhash.hash(null), 0);
  }

  @Test
  public void testUnknownHashFunction() {
    String value = "test";
    // Unknown hash function should default to absHashCode
    HashFunctionSelector.SvHasher unknown = HashFunctionSelector.getSvHasher("unknown");
    int hash = unknown.hash(value);
    int expectedHash = _abshashcode.hash(value);
    Assert.assertEquals(hash, expectedHash);
  }

  @Test
  public void testCaseInsensitive() {
    String value = "test";
    HashFunctionSelector.SvHasher upperCase = HashFunctionSelector.getSvHasher("MURMUR");
    int hash1 = upperCase.hash(value);
    int hash2 = _murmur.hash(value);
    Assert.assertEquals(hash1, hash2);
  }
}
