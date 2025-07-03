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
 * Test for KeySelector implementations with custom hash functions
 */
public class KeySelectorHashFunctionTest {

  @Test
  public void testSingleColumnKeySelectorWithCustomHashFunction() {
    SingleColumnKeySelector selector = new SingleColumnKeySelector(0, "murmur");

    Object[] row = {"test"};
    int hash = selector.computeHash(row);

    // Should be positive
    Assert.assertTrue(hash >= 0);

    // Should use the specified hash function
    Assert.assertEquals(selector.hashAlgorithm(), "murmur");

    // Same input should produce same hash
    int hash2 = selector.computeHash(row);
    Assert.assertEquals(hash, hash2);
  }

  @Test
  public void testMultiColumnKeySelectorWithCustomHashFunction() {
    MultiColumnKeySelector selector = new MultiColumnKeySelector(new int[]{0, 1}, "murmur3");

    Object[] row = {"test1", "test2"};
    int hash = selector.computeHash(row);

    // Should be positive
    Assert.assertTrue(hash >= 0);

    // Should use the specified hash function
    Assert.assertEquals(selector.hashAlgorithm(), "murmur3");

    // Same input should produce same hash
    int hash2 = selector.computeHash(row);
    Assert.assertEquals(hash, hash2);
  }

  @Test
  public void testEmptyKeySelectorWithCustomHashFunction() {
    EmptyKeySelector selector = EmptyKeySelector.getInstance("cityhash");

    Object[] row = {"test"};
    int hash = selector.computeHash(row);

    // Should always return 0
    Assert.assertEquals(hash, 0);

    // Should use the specified hash function
    Assert.assertEquals(selector.hashAlgorithm(), "cityhash");
  }

  @Test
  public void testKeySelectorFactoryWithCustomHashFunction() {
    // Test single column
    KeySelector<?> singleSelector = KeySelectorFactory.getKeySelector(java.util.List.of(0), "murmur");
    Assert.assertEquals(singleSelector.hashAlgorithm(), "murmur");

    // Test multi column
    KeySelector<?> multiSelector = KeySelectorFactory.getKeySelector(java.util.List.of(0, 1), "murmur3");
    Assert.assertEquals(multiSelector.hashAlgorithm(), "murmur3");

    // Test empty
    KeySelector<?> emptySelector = KeySelectorFactory.getKeySelector(java.util.List.of(), "cityhash");
    Assert.assertEquals(emptySelector.hashAlgorithm(), "cityhash");
  }

  @Test
  public void testKeySelectorFactoryWithDefaultHashFunction() {
    // Test single column
    KeySelector<?> singleSelector = KeySelectorFactory.getKeySelector(java.util.List.of(0));
    Assert.assertEquals(singleSelector.hashAlgorithm(), KeySelector.DEFAULT_HASH_ALGORITHM);

    // Test multi column
    KeySelector<?> multiSelector = KeySelectorFactory.getKeySelector(java.util.List.of(0, 1));
    Assert.assertEquals(multiSelector.hashAlgorithm(), KeySelector.DEFAULT_HASH_ALGORITHM);

    // Test empty
    KeySelector<?> emptySelector = KeySelectorFactory.getKeySelector(java.util.List.of());
    Assert.assertEquals(emptySelector.hashAlgorithm(), KeySelector.DEFAULT_HASH_ALGORITHM);
  }

  @Test
  public void testDifferentHashFunctionsProduceDifferentHashes() {
    Object[] row = {"test"};

    SingleColumnKeySelector absSelector = new SingleColumnKeySelector(0, "abshashcode");
    SingleColumnKeySelector murmur2Selector = new SingleColumnKeySelector(0, "murmur");
    SingleColumnKeySelector murmur3Selector = new SingleColumnKeySelector(0, "murmur3");
    SingleColumnKeySelector citySelector = new SingleColumnKeySelector(0, "cityhash");

    int absHash = absSelector.computeHash(row);
    int murmur2Hash = murmur2Selector.computeHash(row);
    int murmur3Hash = murmur3Selector.computeHash(row);
    int cityHash = citySelector.computeHash(row);

    // All hashes should be different
    Assert.assertNotEquals(absHash, murmur2Hash);
    Assert.assertNotEquals(absHash, murmur3Hash);
    Assert.assertNotEquals(absHash, cityHash);
    Assert.assertNotEquals(murmur2Hash, murmur3Hash);
    Assert.assertNotEquals(murmur2Hash, cityHash);
    Assert.assertNotEquals(murmur3Hash, cityHash);
  }
}
