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
package org.apache.pinot.query.runtime.operator.groupby;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for all {@link GroupIdGenerator} implementations produced by {@link GroupIdGeneratorFactory}.
 *
 * <p>These tests serve as a correctness harness for the benchmark scenarios in
 * {@code BenchmarkMSEGroupByKeyGen}: if any generator variant silently breaks, the benchmark results
 * would be meaningless.  Keeping the generators green here ensures the benchmarks measure real work.
 */
public class GroupIdGeneratorTest {

  private static final int NUM_GROUPS_LIMIT = 1_000;
  private static final int INITIAL_CAPACITY = 64;

  // ---- helper factories ----

  private GroupIdGenerator singleIntGen() {
    return GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.INT}, 1, NUM_GROUPS_LIMIT, INITIAL_CAPACITY);
  }

  private GroupIdGenerator singleLongGen() {
    return GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.LONG}, 1, NUM_GROUPS_LIMIT, INITIAL_CAPACITY);
  }

  private GroupIdGenerator singleStringGen() {
    return GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.STRING}, 1, NUM_GROUPS_LIMIT, INITIAL_CAPACITY);
  }

  private GroupIdGenerator twoIntGen() {
    return GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT}, 2, NUM_GROUPS_LIMIT, INITIAL_CAPACITY);
  }

  private GroupIdGenerator fourIntGen() {
    return GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT},
        4, NUM_GROUPS_LIMIT, INITIAL_CAPACITY);
  }

  // ---- single-key correctness ----

  @Test
  public void testSingleIntKeyAssignsUniqueIds() {
    GroupIdGenerator gen = singleIntGen();
    Set<Integer> seenIds = new HashSet<>();
    for (int k = 0; k < 100; k++) {
      int id = gen.getGroupId(k);
      assertNotEquals(id, GroupIdGenerator.INVALID_ID, "Key " + k + " should get a valid id");
      assertTrue(seenIds.add(id), "Group id " + id + " was assigned to more than one key");
    }
    assertEquals(gen.getNumGroups(), 100);
  }

  @Test
  public void testSingleIntKeyIsIdempotent() {
    GroupIdGenerator gen = singleIntGen();
    int first = gen.getGroupId(42);
    int second = gen.getGroupId(42);
    assertEquals(first, second, "Same key must always return the same group id");
    assertEquals(gen.getNumGroups(), 1);
  }

  @Test
  public void testSingleIntKeyNullGroup() {
    GroupIdGenerator gen = singleIntGen();
    int nullId1 = gen.getGroupId(null);
    int nullId2 = gen.getGroupId(null);
    assertNotEquals(nullId1, GroupIdGenerator.INVALID_ID);
    assertEquals(nullId1, nullId2, "null must map to a stable group id");
    assertEquals(gen.getNumGroups(), 1);
  }

  @Test
  public void testSingleStringKeyAssignsUniqueIds() {
    GroupIdGenerator gen = singleStringGen();
    Set<Integer> seenIds = new HashSet<>();
    for (int k = 0; k < 200; k++) {
      String key = "key-" + k;
      int id = gen.getGroupId(key);
      assertNotEquals(id, GroupIdGenerator.INVALID_ID);
      assertTrue(seenIds.add(id), "Duplicate group id for key " + key);
    }
    assertEquals(gen.getNumGroups(), 200);
  }

  @Test
  public void testSingleLongKeyAssignsUniqueIds() {
    GroupIdGenerator gen = singleLongGen();
    for (int k = 0; k < 50; k++) {
      int id = gen.getGroupId((long) k);
      assertNotEquals(id, GroupIdGenerator.INVALID_ID);
    }
    assertEquals(gen.getNumGroups(), 50);
  }

  // ---- two-key correctness ----

  @Test
  public void testTwoIntKeysAssignUniqueIds() {
    GroupIdGenerator gen = twoIntGen();
    Set<Integer> seenIds = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        int id = gen.getGroupId(new Object[]{i, j});
        assertNotEquals(id, GroupIdGenerator.INVALID_ID);
        assertTrue(seenIds.add(id), "Duplicate id for (" + i + "," + j + ")");
      }
    }
    assertEquals(gen.getNumGroups(), 100);
  }

  @Test
  public void testTwoIntKeysIsIdempotent() {
    GroupIdGenerator gen = twoIntGen();
    int first = gen.getGroupId(new Object[]{3, 7});
    int second = gen.getGroupId(new Object[]{3, 7});
    assertEquals(first, second);

    // Different combination must produce a different id
    int other = gen.getGroupId(new Object[]{7, 3});
    assertNotEquals(first, other, "Different key combinations must produce different ids");
  }

  @Test
  public void testTwoIntKeysNullComponent() {
    GroupIdGenerator gen = twoIntGen();
    int id1 = gen.getGroupId(new Object[]{null, 5});
    int id2 = gen.getGroupId(new Object[]{null, 5});
    assertEquals(id1, id2, "null component must produce a stable id");

    int id3 = gen.getGroupId(new Object[]{null, 6});
    assertNotEquals(id1, id3, "(null,5) and (null,6) must be different groups");
  }

  // ---- four-key (multi-key) correctness ----

  @Test
  public void testFourIntKeysAssignUniqueIds() {
    GroupIdGenerator gen = fourIntGen();
    Set<Integer> seenIds = new HashSet<>();
    for (int a = 0; a < 5; a++) {
      for (int b = 0; b < 5; b++) {
        for (int c = 0; c < 5; c++) {
          for (int d = 0; d < 5; d++) {
            int id = gen.getGroupId(new Object[]{a, b, c, d});
            assertNotEquals(id, GroupIdGenerator.INVALID_ID);
            assertTrue(seenIds.add(id),
                "Duplicate id for (" + a + "," + b + "," + c + "," + d + ")");
          }
        }
      }
    }
    assertEquals(gen.getNumGroups(), 625); // 5^4
  }

  // ---- group limit enforcement ----

  @Test
  public void testGroupLimitEnforced() {
    int limit = 5;
    GroupIdGenerator gen = GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.INT}, 1, limit, INITIAL_CAPACITY);

    for (int k = 0; k < limit; k++) {
      assertNotEquals(gen.getGroupId(k), GroupIdGenerator.INVALID_ID, "Key " + k + " should be accepted");
    }
    assertEquals(gen.getNumGroups(), limit);

    // One more distinct key must be rejected
    int overflowId = gen.getGroupId(limit + 100);
    assertEquals(overflowId, GroupIdGenerator.INVALID_ID, "New key beyond limit must return INVALID_ID");
    assertEquals(gen.getNumGroups(), limit, "NumGroups must not grow beyond limit");
  }

  @Test
  public void testGroupLimitReturnsPreviousIdForKnownKey() {
    int limit = 3;
    GroupIdGenerator gen = GroupIdGeneratorFactory.getGroupIdGenerator(
        new ColumnDataType[]{ColumnDataType.INT}, 1, limit, INITIAL_CAPACITY);

    int id0 = gen.getGroupId(0);
    int id1 = gen.getGroupId(1);
    int id2 = gen.getGroupId(2);
    // Fill the limit
    assertEquals(gen.getNumGroups(), limit);

    // Existing keys must still resolve even after the limit is reached
    assertEquals(gen.getGroupId(0), id0);
    assertEquals(gen.getGroupId(1), id1);
    assertEquals(gen.getGroupId(2), id2);
  }

  // ---- group key iterator correctness ----

  @Test
  public void testGroupKeyIteratorCoversAllGroups() {
    GroupIdGenerator gen = singleIntGen();
    int numDistinct = 50;
    Map<Integer, Integer> keyToExpectedId = new HashMap<>();
    for (int k = 0; k < numDistinct; k++) {
      int id = gen.getGroupId(k);
      keyToExpectedId.put(k, id);
    }

    // The iterator must emit exactly numDistinct entries
    Set<Integer> visitedIds = new HashSet<>();
    int numColumns = 2; // [key col, agg col placeholder]
    Iterator<GroupIdGenerator.GroupKey> it = gen.getGroupKeyIterator(numColumns);
    while (it.hasNext()) {
      GroupIdGenerator.GroupKey gk = it.next();
      assertTrue(visitedIds.add(gk._groupId), "Group id " + gk._groupId + " appeared twice in iterator");
      // Verify the key value reconstructed in _row[0] is the original integer
      Integer origKey = (Integer) gk._row[0];
      assertNotEquals(origKey, null);
      assertEquals((int) gk._groupId, (int) keyToExpectedId.get(origKey),
          "Iterator group id mismatch for key " + origKey);
    }
    assertEquals(visitedIds.size(), numDistinct);
  }

  @Test
  public void testMultiKeyIteratorCoversAllGroups() {
    GroupIdGenerator gen = twoIntGen();
    List<int[]> insertedKeys = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        gen.getGroupId(new Object[]{i, j});
        insertedKeys.add(new int[]{i, j});
      }
    }
    assertEquals(gen.getNumGroups(), 25);

    Set<Integer> visitedIds = new HashSet<>();
    Iterator<GroupIdGenerator.GroupKey> it = gen.getGroupKeyIterator(3);
    while (it.hasNext()) {
      GroupIdGenerator.GroupKey gk = it.next();
      visitedIds.add(gk._groupId);
      // row[0] and row[1] must be non-null integers
      assertTrue(gk._row[0] instanceof Integer, "First key component must be Integer");
      assertTrue(gk._row[1] instanceof Integer, "Second key component must be Integer");
    }
    assertEquals(visitedIds.size(), 25);
  }

  // ---- skewed key pattern ----

  @Test
  public void testSkewedKeysLowCardinality() {
    // 100 rows, only 3 distinct groups — simulates hot-key skew
    GroupIdGenerator gen = singleIntGen();
    int[] ids = new int[100];
    for (int r = 0; r < 100; r++) {
      ids[r] = gen.getGroupId(r % 3);
    }
    assertEquals(gen.getNumGroups(), 3);
    // All ids for the same key modulo must be equal
    for (int r = 3; r < 100; r++) {
      assertEquals(ids[r], ids[r % 3], "Row " + r + " should have same id as row " + (r % 3));
    }
  }

  // ---- factory routing correctness ----

  @DataProvider(name = "factoryScenarios")
  public Object[][] factoryScenarios() {
    return new Object[][]{
        {new ColumnDataType[]{ColumnDataType.INT}, 1, "OneIntKeyGroupIdGenerator"},
        {new ColumnDataType[]{ColumnDataType.LONG}, 1, "OneLongKeyGroupIdGenerator"},
        {new ColumnDataType[]{ColumnDataType.FLOAT}, 1, "OneFloatKeyGroupIdGenerator"},
        {new ColumnDataType[]{ColumnDataType.DOUBLE}, 1, "OneDoubleKeyGroupIdGenerator"},
        {new ColumnDataType[]{ColumnDataType.STRING}, 1, "OneObjectKeyGroupIdGenerator"},
        {new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT}, 2, "TwoKeysGroupIdGenerator"},
        {new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.INT, ColumnDataType.INT}, 3,
            "MultiKeysGroupIdGenerator"},
    };
  }

  @Test(dataProvider = "factoryScenarios")
  public void testFactorySelectsCorrectBackend(ColumnDataType[] keyTypes, int numKeys, String expectedClassName) {
    GroupIdGenerator gen = GroupIdGeneratorFactory.getGroupIdGenerator(
        keyTypes, numKeys, NUM_GROUPS_LIMIT, INITIAL_CAPACITY);
    assertEquals(gen.getClass().getSimpleName(), expectedClassName,
        "Factory should select " + expectedClassName + " for " + numKeys + " key(s) of type " + keyTypes[0]);
  }
}
