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
package org.apache.pinot.core.util;

import java.lang.reflect.Field;
import java.util.concurrent.Executors;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.HashUtil;
import org.apache.pinot.core.data.table.IndexedTable;
import org.apache.pinot.core.data.table.SimpleIndexedTable;
import org.apache.pinot.core.operator.blocks.results.GroupByResultsBlock;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;


public class GroupByUtilsTest {

  @Test
  public void testGetTableCapacity() {
    assertEquals(GroupByUtils.getTableCapacity(0), 5000);
    assertEquals(GroupByUtils.getTableCapacity(1), 5000);
    assertEquals(GroupByUtils.getTableCapacity(1000), 5000);
    assertEquals(GroupByUtils.getTableCapacity(10000), 50000);
    assertEquals(GroupByUtils.getTableCapacity(100000), 500000);
    assertEquals(GroupByUtils.getTableCapacity(1000000), 5000000);
    assertEquals(GroupByUtils.getTableCapacity(10000000), 50000000);
    assertEquals(GroupByUtils.getTableCapacity(100000000), 500000000);
    assertEquals(GroupByUtils.getTableCapacity(1000000000), Integer.MAX_VALUE);
  }

  @Test
  public void getIndexedTableTrimThreshold() {
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, -1), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 0), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10000), 10000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100000), 100000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000), 1000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 10000000), 10000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 100000000), 100000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000000), 1000000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(5000, 1000000001), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(Integer.MAX_VALUE, 10), Integer.MAX_VALUE);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(500000000, 10), 1000000000);
    assertEquals(GroupByUtils.getIndexedTableTrimThreshold(500000001, 10), Integer.MAX_VALUE);
  }

  /**
   * Verifies that {@link GroupByUtils#createIndexedTableForCombineOperator} with an explicit
   * {@code groupTrimThresholdOverride} correctly propagates or preserves disabled-trim sentinels.
   * <ul>
   *   <li>A non-positive override (0, -1) or one above MAX_TRIM_THRESHOLD must produce a trim-disabled table
   *       ({@code _trimThreshold == Integer.MAX_VALUE}), not silently enable trimming at threshold = 1.</li>
   *   <li>A valid positive override within range must produce a trim-enabled table at that threshold.</li>
   * </ul>
   */
  @Test
  public void testCreateIndexedTableForCombineOperatorTrimThresholdOverride()
      throws Exception {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) LIMIT 100");
    queryContext.setMinServerGroupTrimSize(5000); // trimSize = max(100*5, 5000) = 5000
    DataSchema dataSchema = new DataSchema(new String[]{"a", "sum(b)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    GroupByResultsBlock block = new GroupByResultsBlock(dataSchema, queryContext);

    // Disabled-trim sentinel: 0 must NOT be converted to 1 (which would enable trim).
    queryContext.setGroupTrimThreshold(0); // normally disabled
    IndexedTable tableZero = GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, 1,
        Executors.newSingleThreadExecutor(), 100, 0);
    assertTrue(tableZero instanceof SimpleIndexedTable, "Expected SimpleIndexedTable for numThreads=1");
    assertEquals(trimThresholdOf(tableZero), Integer.MAX_VALUE,
        "groupTrimThresholdOverride=0 must produce a trim-disabled table");

    // Disabled-trim sentinel: negative value.
    IndexedTable tableNeg = GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, 1,
        Executors.newSingleThreadExecutor(), 100, -1);
    assertEquals(trimThresholdOf(tableNeg), Integer.MAX_VALUE,
        "groupTrimThresholdOverride=-1 must produce a trim-disabled table");

    // Disabled-trim sentinel: above MAX_TRIM_THRESHOLD.
    IndexedTable tableHigh = GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, 1,
        Executors.newSingleThreadExecutor(), 100, GroupByUtils.MAX_TRIM_THRESHOLD + 1);
    assertEquals(trimThresholdOf(tableHigh), Integer.MAX_VALUE,
        "groupTrimThresholdOverride above MAX_TRIM_THRESHOLD must produce a trim-disabled table");

    // Valid override: should produce a trim-enabled table with the scaled threshold.
    // trimSize=5000, override=100_000 → getIndexedTableTrimThreshold(5000, 100_000) = 100_000
    queryContext.setGroupTrimThreshold(1_000_000); // original, not used
    IndexedTable tableValid = GroupByUtils.createIndexedTableForCombineOperator(block, queryContext, 1,
        Executors.newSingleThreadExecutor(), 100, 100_000);
    assertFalse(trimThresholdOf(tableValid) == Integer.MAX_VALUE,
        "Valid groupTrimThresholdOverride must produce a trim-enabled table");
    assertEquals(trimThresholdOf(tableValid), 100_000,
        "groupTrimThresholdOverride=100_000 with trimSize=5000 must produce trimThreshold=100_000");
  }

  private static int trimThresholdOf(IndexedTable table)
      throws Exception {
    Field f = IndexedTable.class.getDeclaredField("_trimThreshold");
    f.setAccessible(true);
    return (int) f.get(table);
  }

  private static int trimSizeOf(IndexedTable table)
      throws Exception {
    Field f = IndexedTable.class.getDeclaredField("_trimSize");
    f.setAccessible(true);
    return (int) f.get(table);
  }

  /**
   * Verifies {@link GroupByUtils#getEffectiveCombineTrimThreshold}:
   * <ul>
   *   <li>Queries without ORDER BY always return MAX_VALUE (trim is meaningless).</li>
   *   <li>Disabled-trim sentinels (0, -1, above MAX_TRIM_THRESHOLD) return MAX_VALUE.</li>
   *   <li>A valid threshold returns {@code max(threshold, 2 * trimSize)}.</li>
   * </ul>
   */
  @Test
  public void testGetEffectiveCombineTrimThreshold() {
    // No ORDER BY: always disabled regardless of threshold setting
    QueryContext noOrderBy =
        QueryContextConverterUtils.getQueryContext("SELECT a, SUM(b) FROM t GROUP BY a LIMIT 100");
    noOrderBy.setGroupTrimThreshold(1_000_000);
    assertEquals(GroupByUtils.getEffectiveCombineTrimThreshold(noOrderBy), Integer.MAX_VALUE,
        "No ORDER BY must return MAX_VALUE");

    // With ORDER BY, disabled sentinels
    QueryContext withOrderBy = QueryContextConverterUtils.getQueryContext(
        "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) LIMIT 100");
    withOrderBy.setMinServerGroupTrimSize(5000);

    withOrderBy.setGroupTrimThreshold(0);
    assertEquals(GroupByUtils.getEffectiveCombineTrimThreshold(withOrderBy), Integer.MAX_VALUE,
        "groupTrimThreshold=0 must return MAX_VALUE");

    withOrderBy.setGroupTrimThreshold(-1);
    assertEquals(GroupByUtils.getEffectiveCombineTrimThreshold(withOrderBy), Integer.MAX_VALUE,
        "groupTrimThreshold=-1 must return MAX_VALUE");

    withOrderBy.setGroupTrimThreshold(GroupByUtils.MAX_TRIM_THRESHOLD + 1);
    assertEquals(GroupByUtils.getEffectiveCombineTrimThreshold(withOrderBy), Integer.MAX_VALUE,
        "groupTrimThreshold above MAX must return MAX_VALUE");

    // Valid threshold: result = max(threshold, 2*trimSize); trimSize=max(100*5,5000)=5000 → 2*trimSize=10000
    withOrderBy.setGroupTrimThreshold(1_000_000);
    assertEquals(GroupByUtils.getEffectiveCombineTrimThreshold(withOrderBy), 1_000_000,
        "valid threshold must be returned as-is when above 2*trimSize");

    withOrderBy.setGroupTrimThreshold(100);
    assertEquals(GroupByUtils.getEffectiveCombineTrimThreshold(withOrderBy), 10_000,
        "threshold below 2*trimSize must be raised to 2*trimSize=10000");
  }

  /**
   * Verifies {@link GroupByUtils#createPartitionTableForCombineOperator}:
   * <ul>
   *   <li>The returned table must be a {@link SimpleIndexedTable} (single-threaded partition table).</li>
   *   <li>Auto-trim must be disabled: {@code _trimThreshold == Integer.MAX_VALUE}.</li>
   *   <li>For ORDER BY queries, {@code _trimSize} must equal the effective trim size so explicit
   *       {@link IndexedTable#trim()} calls work correctly.</li>
   * </ul>
   */
  @Test
  public void testCreatePartitionTableForCombineOperator()
      throws Exception {
    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(
        "SELECT a, SUM(b) FROM t GROUP BY a ORDER BY SUM(b) LIMIT 100");
    queryContext.setMinServerGroupTrimSize(5000);
    queryContext.setGroupTrimThreshold(1_000_000);
    DataSchema dataSchema = new DataSchema(new String[]{"a", "sum(b)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.DOUBLE});
    GroupByResultsBlock block = new GroupByResultsBlock(dataSchema, queryContext);

    IndexedTable table = GroupByUtils.createPartitionTableForCombineOperator(block, queryContext, 1000,
        Executors.newSingleThreadExecutor());

    assertTrue(table instanceof SimpleIndexedTable, "Partition table must be a SimpleIndexedTable");
    assertEquals(trimThresholdOf(table), Integer.MAX_VALUE, "Partition table must have auto-trim disabled");
    // trimSize = max(100*5, 5000) = 5000; explicit trim() calls reduce the table to this size
    int expectedTrimSize =
        GroupByUtils.getTableCapacity(queryContext.getLimit(), queryContext.getMinServerGroupTrimSize());
    assertNotEquals(trimSizeOf(table), Integer.MAX_VALUE,
        "Partition table must have a valid trimSize for explicit trim() calls");
    assertEquals(trimSizeOf(table), expectedTrimSize,
        "Partition table trimSize must equal the effective combine trim size");
  }

  @Test
  public void testGetIndexedTableInitialCapacity() {
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 10, 128), 128);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 100, 128),
        HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 100, 256), 256);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(Integer.MAX_VALUE, 1000, 256),
        HashUtil.getHashMapCapacity(1000));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 10, 128), 128);
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 10, 256), HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 100, 256), HashUtil.getHashMapCapacity(100));
    assertEquals(GroupByUtils.getIndexedTableInitialCapacity(100, 1000, 256), HashUtil.getHashMapCapacity(100));
  }
}
