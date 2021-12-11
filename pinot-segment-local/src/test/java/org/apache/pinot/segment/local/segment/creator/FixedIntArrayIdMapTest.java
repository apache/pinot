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
package org.apache.pinot.segment.local.segment.creator;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.util.Random;
import org.apache.pinot.segment.local.io.writer.impl.DirectMemoryManager;
import org.apache.pinot.segment.local.utils.FixedIntArrayOffHeapIdMap;
import org.apache.pinot.segment.local.utils.IdMap;
import org.apache.pinot.spi.utils.FixedIntArray;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link FixedIntArrayOffHeapIdMap}
 */
public class FixedIntArrayIdMapTest {
  private static final int NUM_ROWS = 10001;
  private static final int NUM_COLUMNS = 3;
  private static final int INITIAL_CARDINALITY = 23;
  private static final int ON_HEAP_CACHE_SIZE = 10;
  private static final Random RANDOM = new Random();

  private DirectMemoryManager _memoryManager;
  private IdMap<FixedIntArray> _idMap;

  @BeforeClass
  public void setup() {
    _memoryManager = new DirectMemoryManager(FixedIntArrayIdMapTest.class.getName());
    _idMap = new FixedIntArrayOffHeapIdMap(INITIAL_CARDINALITY, ON_HEAP_CACHE_SIZE, NUM_COLUMNS, _memoryManager,
        FixedIntArrayIdMapTest.class.getName());
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _idMap.close();
    _memoryManager.close();
  }

  /**
   * This test indexes a specified number of values in the class being tested and checks correctness by
   * comparing results against {@link BiMap} for the same input.
   * <ul>
   *   <li> Size of the map (cardinality) should be as expected. </li>
   *   <li> For each value, id should be as expected. </li>
   *   <li> For each id, should return the expected value back. </li>
   * </ul>
   */
  @Test
  public void test() {
    BiMap<FixedIntArray, Integer> expectedMap = addValues(_idMap);
    int numValues = expectedMap.size();

    // Test invalid Value
    Assert.assertEquals(_idMap.getId(new FixedIntArray(new int[]{})), IdMap.INVALID_ID);

    Assert.assertEquals(_idMap.size(), numValues);
    testValues(expectedMap);
  }

  private void testValues(BiMap<FixedIntArray, Integer> map) {
    for (int dictId = 0; dictId < _idMap.size(); dictId++) {
      FixedIntArray actual = _idMap.getKey(dictId);
      FixedIntArray expected = map.inverse().get(dictId);

      Assert.assertEquals(actual, expected);
      Assert.assertEquals(_idMap.getId(actual), map.get(expected).intValue());
    }
  }

  private BiMap<FixedIntArray, Integer> addValues(IdMap<FixedIntArray> idMap) {
    BiMap<FixedIntArray, Integer> map = HashBiMap.create();
    int numValues = 0;

    for (int row = 0; row < NUM_ROWS; row++) {
      int[] values = new int[NUM_COLUMNS];
      for (int col = 0; col < NUM_COLUMNS; col++) {
        values[col] = RANDOM.nextInt(10); // Max of 1000 unique values possible, so there will be duplicates.
      }
      FixedIntArray value = new FixedIntArray(values);
      idMap.put(value);

      if (!map.containsKey(value)) {
        map.put(value, numValues++);
      }
    }
    return map;
  }
}
