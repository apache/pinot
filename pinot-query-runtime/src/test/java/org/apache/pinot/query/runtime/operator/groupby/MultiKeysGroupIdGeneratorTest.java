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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.operator.groupby.GroupIdGenerator.GroupKey;
import org.testng.annotations.Test;

import static org.apache.pinot.query.runtime.operator.groupby.GroupIdGenerator.INVALID_ID;
import static org.testng.Assert.assertEquals;


public class MultiKeysGroupIdGeneratorTest {
  private static final ColumnDataType[] KEY_TYPES = {ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.LONG};
  private static final int NUM_KEY_COLUMNS = KEY_TYPES.length;
  // Number of columns in the output row, which includes the key columns and one aggregation column
  private static final int NUM_COLUMNS = NUM_KEY_COLUMNS + 1;

  @Test
  public void testGroupIds() {
    GroupIdGenerator generator = new MultiKeysGroupIdGenerator(KEY_TYPES, NUM_KEY_COLUMNS, 100, 10);
    // The caller reuses the same array for all the rows
    Object[] key = new Object[NUM_KEY_COLUMNS];

    assertEquals(getGroupId(generator, key, "a", 1, 10L), 0);
    assertEquals(getGroupId(generator, key, "b", 2, 20L), 1);
    // Same keys should map to the same group ids
    assertEquals(getGroupId(generator, key, "a", 1, 10L), 0);
    assertEquals(getGroupId(generator, key, "b", 2, 20L), 1);
    // Keys sharing a prefix with an existing group should get new group ids
    assertEquals(getGroupId(generator, key, "a", 1, 20L), 2);
    assertEquals(getGroupId(generator, key, "a", 2, 10L), 3);
    assertEquals(generator.getNumGroups(), 4);

    assertEquals(getGroupKeys(generator), Map.of(
        0, Arrays.asList("a", 1, 10L, null),
        1, Arrays.asList("b", 2, 20L, null),
        2, Arrays.asList("a", 1, 20L, null),
        3, Arrays.asList("a", 2, 10L, null)));
  }

  @Test
  public void testNullKeyValues() {
    GroupIdGenerator generator = new MultiKeysGroupIdGenerator(KEY_TYPES, NUM_KEY_COLUMNS, 100, 10);
    Object[] key = new Object[NUM_KEY_COLUMNS];

    assertEquals(getGroupId(generator, key, null, 1, 10L), 0);
    assertEquals(getGroupId(generator, key, "a", null, 10L), 1);
    assertEquals(getGroupId(generator, key, null, 1, 10L), 0);
    assertEquals(getGroupId(generator, key, null, null, null), 2);
    assertEquals(generator.getNumGroups(), 3);

    assertEquals(getGroupKeys(generator), Map.of(
        0, Arrays.asList(null, 1, 10L, null),
        1, Arrays.asList("a", null, 10L, null),
        2, Arrays.asList(null, null, null, null)));
  }

  @Test
  public void testNumGroupsLimit() {
    GroupIdGenerator generator = new MultiKeysGroupIdGenerator(KEY_TYPES, NUM_KEY_COLUMNS, 2, 10);
    Object[] key = new Object[NUM_KEY_COLUMNS];

    assertEquals(getGroupId(generator, key, "a", 1, 10L), 0);
    assertEquals(getGroupId(generator, key, "b", 2, 20L), 1);

    // Limit is reached, new keys should not be added
    assertEquals(getGroupId(generator, key, "c", 3, 30L), INVALID_ID);
    // The first key value is known, but the second one is not
    assertEquals(getGroupId(generator, key, "a", 3, 30L), INVALID_ID);
    // All the key values are known, but the combination is not an existing group
    assertEquals(getGroupId(generator, key, "a", 2, 20L), INVALID_ID);
    // Existing groups should still be resolved after the failed lookups
    assertEquals(getGroupId(generator, key, "a", 1, 10L), 0);
    assertEquals(getGroupId(generator, key, "b", 2, 20L), 1);
    assertEquals(generator.getNumGroups(), 2);

    assertEquals(getGroupKeys(generator), Map.of(
        0, Arrays.asList("a", 1, 10L, null),
        1, Arrays.asList("b", 2, 20L, null)));
  }

  /// Every row creates a new group, which forces the map to grow while the keys are generated from the same reusable
  /// array.
  @Test
  public void testManyGroups() {
    int numGroups = 1000;
    GroupIdGenerator generator = new MultiKeysGroupIdGenerator(KEY_TYPES, NUM_KEY_COLUMNS, numGroups, 10);
    Object[] key = new Object[NUM_KEY_COLUMNS];

    for (int i = 0; i < numGroups; i++) {
      assertEquals(getGroupId(generator, key, "k" + i, i, (long) i), i);
    }
    // Replay the same rows in reverse order
    for (int i = numGroups - 1; i >= 0; i--) {
      assertEquals(getGroupId(generator, key, "k" + i, i, (long) i), i);
    }
    assertEquals(generator.getNumGroups(), numGroups);

    Map<Integer, List<Object>> groupKeys = getGroupKeys(generator);
    assertEquals(groupKeys.size(), numGroups);
    for (int i = 0; i < numGroups; i++) {
      assertEquals(groupKeys.get(i), Arrays.asList("k" + i, i, (long) i, null));
    }
  }

  private static int getGroupId(GroupIdGenerator generator, Object[] key, Object... keyValues) {
    System.arraycopy(keyValues, 0, key, 0, NUM_KEY_COLUMNS);
    return generator.getGroupId(key);
  }

  private static Map<Integer, List<Object>> getGroupKeys(GroupIdGenerator generator) {
    Map<Integer, List<Object>> groupKeys = new HashMap<>();
    Iterator<GroupKey> iterator = generator.getGroupKeyIterator(NUM_COLUMNS);
    while (iterator.hasNext()) {
      GroupKey groupKey = iterator.next();
      groupKeys.put(groupKey._groupId, Arrays.asList(groupKey._row));
    }
    return groupKeys;
  }
}
