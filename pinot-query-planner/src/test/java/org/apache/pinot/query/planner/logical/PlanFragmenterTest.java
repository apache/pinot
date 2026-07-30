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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.plannode.AggregateNode;
import org.apache.pinot.query.planner.plannode.ExchangeNode;
import org.apache.pinot.query.planner.plannode.FilterNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.ProjectNode;
import org.apache.pinot.query.planner.plannode.SortNode;
import org.apache.pinot.query.planner.plannode.TableScanNode;
import org.apache.pinot.query.planner.plannode.WindowNode;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


/**
 * Tests the {@code sortedOnSender} marking that {@link PlanFragmenter} applies when the
 * {@code sortedSelectionMergeEnabled} query option is on.
 *
 * <p>This marking is the only gate between the query option and the k-way merge in
 * {@code SortedMailboxReceiveOperator}, and a false positive is not a crash: the merge assumes each mailbox stream is
 * sorted, nothing downstream re-sorts, and under {@code LIMIT}/{@code OFFSET} the query simply returns the wrong rows.
 * So every reject branch is pinned here, not just the accept path.
 */
public class PlanFragmenterTest {
  private static final String OFFLINE_ONLY_TABLE = "offlineOnlyTable";
  private static final String REALTIME_ONLY_TABLE = "realtimeOnlyTable";
  private static final String HYBRID_TABLE = "hybridTable";
  private static final String LOGICAL_TABLE = "logicalTable";
  private static final String UNKNOWN_TABLE = "unknownTable";

  private static final DataSchema SCHEMA =
      new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{
          DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING
      });
  private static final List<RelFieldCollation> COLLATION = List.of(new RelFieldCollation(0));

  // --------------------------------------------------------------------------------------------------------------
  // Sender-fragment shapes
  // --------------------------------------------------------------------------------------------------------------

  @DataProvider(name = "senderShapes")
  public Object[][] senderShapes() {
    return new Object[][]{
        // {description, sender fragment root, expected sortedOnSender when the option is ON}
        {"sort over scan", (Supplier<PlanNode>) () -> sort(scan(OFFLINE_ONLY_TABLE)), true},
        {"sort over project over scan", (Supplier<PlanNode>) () -> sort(project(scan(OFFLINE_ONLY_TABLE))), true},
        {"sort over two projects over scan",
            (Supplier<PlanNode>) () -> sort(project(project(scan(OFFLINE_ONLY_TABLE)))), true},
        {"realtime-only table", (Supplier<PlanNode>) () -> sort(scan(REALTIME_ONLY_TABLE)), true},
        {"explicit OFFLINE type suffix", (Supplier<PlanNode>) () -> sort(scan(HYBRID_TABLE + "_OFFLINE")), true},
        {"explicit REALTIME type suffix", (Supplier<PlanNode>) () -> sort(scan(HYBRID_TABLE + "_REALTIME")), true},

        // Rejected: not a leaf selection ORDER BY.
        {"bare scan (no sort)", (Supplier<PlanNode>) () -> scan(OFFLINE_ONLY_TABLE), false},
        {"project root", (Supplier<PlanNode>) () -> project(scan(OFFLINE_ONLY_TABLE)), false},
        {"filter in the chain", (Supplier<PlanNode>) () -> sort(filter(scan(OFFLINE_ONLY_TABLE))), false},
        {"aggregate below sort", (Supplier<PlanNode>) () -> sort(aggregate(scan(OFFLINE_ONLY_TABLE))), false},
        {"window below sort", (Supplier<PlanNode>) () -> sort(window(scan(OFFLINE_ONLY_TABLE))), false},
        {"join below sort",
            (Supplier<PlanNode>) () -> sort(join(scan(OFFLINE_ONLY_TABLE), scan(REALTIME_ONLY_TABLE))), false},

        // Rejected: the scan does not resolve to exactly one physical table, so a single mailbox stream can carry two
        // independently sorted runs (hybrid) or several tables (logical).
        {"hybrid table", (Supplier<PlanNode>) () -> sort(scan(HYBRID_TABLE)), false},
        {"logical table", (Supplier<PlanNode>) () -> sort(scan(LOGICAL_TABLE)), false},
        {"unknown table", (Supplier<PlanNode>) () -> sort(scan(UNKNOWN_TABLE)), false}
    };
  }

  @Test(dataProvider = "senderShapes")
  public void testSortedOnSenderMarkingByShape(String description, Supplier<PlanNode> senderRoot,
      boolean expectedWhenEnabled) {
    assertEquals(fragmentAndGetSortedOnSender(senderRoot.get(), COLLATION, true), expectedWhenEnabled, description);
  }

  @Test(dataProvider = "senderShapes")
  public void testNoMarkingWhenOptionDisabled(String description, Supplier<PlanNode> senderRoot,
      boolean expectedWhenEnabled) {
    // With the option off the flag must always equal the exchange's own sortOnSender (false here), regardless of shape.
    assertFalse(fragmentAndGetSortedOnSender(senderRoot.get(), COLLATION, false), description);
  }

  // --------------------------------------------------------------------------------------------------------------
  // Collation matching
  // --------------------------------------------------------------------------------------------------------------

  @DataProvider(name = "collations")
  public Object[][] collations() {
    RelFieldCollation asc0 = new RelFieldCollation(0);
    RelFieldCollation desc0 =
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST);
    RelFieldCollation asc0NullsFirst =
        new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.FIRST);
    RelFieldCollation asc1 = new RelFieldCollation(1);
    return new Object[][]{
        {"identical single key", List.of(asc0), List.of(asc0), true},
        {"identical two keys", List.of(asc0, asc1), List.of(asc0, asc1), true},
        {"identical descending", List.of(desc0), List.of(desc0), true},
        {"different field index", List.of(asc0), List.of(asc1), false},
        {"different direction", List.of(asc0), List.of(desc0), false},
        {"different null direction", List.of(asc0), List.of(asc0NullsFirst), false},
        {"different size", List.of(asc0, asc1), List.of(asc0), false},
        {"exchange has no collation", List.of(asc0), null, false},
        // A collation-less LogicalSort (plain `SELECT ... LIMIT n`, no ORDER BY) below a collation-less sort exchange.
        // Both lists are empty, so a naive element-wise comparison would call them "matching" and mark the receive as
        // sorted-on-sender; SortedMailboxReceiveOperator then rejects the empty collation and the query fails.
        {"both collations empty", List.of(), List.of(), false},
        {"sort has no collation, exchange does", List.of(), List.of(asc0), false}
    };
  }

  @Test(dataProvider = "collations")
  public void testSortedOnSenderMarkingByCollation(String description, List<RelFieldCollation> sortCollation,
      @Nullable List<RelFieldCollation> exchangeCollation, boolean expected) {
    PlanNode senderRoot = new SortNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, mutable(scan(OFFLINE_ONLY_TABLE)),
        sortCollation, 10, 0);
    assertEquals(fragmentAndGetSortedOnSender(senderRoot, exchangeCollation, true), expected, description);
  }

  /**
   * When the exchange itself already declares {@code sortOnSender}, the flag must stay set no matter what the option or
   * the shape gate says: the marking is an additional source of truth, never a filter on the existing one.
   */
  @Test
  public void testExistingSortOnSenderIsPreserved() {
    // A shape the gate rejects (hybrid table), with sortOnSender declared on the exchange.
    assertTrue(fragmentAndGetSortedOnSender(sort(scan(HYBRID_TABLE)), COLLATION, false, true));
    assertTrue(fragmentAndGetSortedOnSender(sort(scan(HYBRID_TABLE)), COLLATION, true, true));
  }

  /**
   * A null {@link TableCache} must fail closed: no table can be proven single-physical, so nothing is marked.
   */
  @Test
  public void testNullTableCacheFailsClosed() {
    PlanNode receiverRoot = exchange(sort(scan(OFFLINE_ONLY_TABLE)), COLLATION, false);
    PlanFragmenter fragmenter = new PlanFragmenter(true, null);
    PlanNode result = receiverRoot.visit(fragmenter, fragmenter.createContext());
    assertFalse(((MailboxReceiveNode) result).isSortedOnSender());
  }

  // --------------------------------------------------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------------------------------------------------

  private boolean fragmentAndGetSortedOnSender(PlanNode senderRoot, @Nullable List<RelFieldCollation> exchangeCollation,
      boolean optionEnabled) {
    return fragmentAndGetSortedOnSender(senderRoot, exchangeCollation, optionEnabled, false);
  }

  private boolean fragmentAndGetSortedOnSender(PlanNode senderRoot, @Nullable List<RelFieldCollation> exchangeCollation,
      boolean optionEnabled, boolean exchangeSortOnSender) {
    PlanNode receiverRoot = exchange(senderRoot, exchangeCollation, exchangeSortOnSender);
    PlanFragmenter fragmenter = new PlanFragmenter(optionEnabled, mockTableCache());
    PlanNode result = receiverRoot.visit(fragmenter, fragmenter.createContext());
    return ((MailboxReceiveNode) result).isSortedOnSender();
  }

  private static ExchangeNode exchange(PlanNode input, @Nullable List<RelFieldCollation> collations,
      boolean sortOnSender) {
    return new ExchangeNode(0, SCHEMA, mutable(input), PinotRelExchangeType.getDefaultExchangeType(),
        RelDistribution.Type.HASH_DISTRIBUTED, List.of(0), false, collations, sortOnSender, false, null, null,
        "absHashCode");
  }

  private static SortNode sort(PlanNode input) {
    return new SortNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, mutable(input), COLLATION, 10, 0);
  }

  private static ProjectNode project(PlanNode input) {
    return new ProjectNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, mutable(input), List.of());
  }

  private static FilterNode filter(PlanNode input) {
    return new FilterNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, mutable(input), null);
  }

  private static AggregateNode aggregate(PlanNode input) {
    return new AggregateNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, mutable(input), List.of(), List.of(), List.of(0),
        AggregateNode.AggType.DIRECT, false, null, 0);
  }

  private static WindowNode window(PlanNode input) {
    return new WindowNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, mutable(input), List.of(0), COLLATION, List.of(),
        WindowNode.WindowFrameType.ROWS, Integer.MIN_VALUE, Integer.MAX_VALUE, WindowNode.WindowExclusion.NO_OTHERS,
        List.of());
  }

  private static JoinNode join(PlanNode left, PlanNode right) {
    return new JoinNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, new ArrayList<>(List.of(left, right)),
        JoinRelType.INNER, List.of(0), List.of(0), List.of(), JoinNode.JoinStrategy.HASH);
  }

  private static List<PlanNode> mutable(PlanNode input) {
    return new ArrayList<>(List.of(input));
  }

  private static TableScanNode scan(String tableName) {
    return new TableScanNode(0, SCHEMA, PlanNode.NodeHint.EMPTY, new ArrayList<>(), tableName, List.of("col1", "col2"));
  }

  /**
   * A table cache holding one offline-only table, one realtime-only table, one hybrid table and one logical table.
   */
  private static TableCache mockTableCache() {
    TableCache tableCache = Mockito.mock(TableCache.class);
    Function<String, String> actualName = name -> {
      String raw = name.replace("_OFFLINE", "").replace("_REALTIME", "");
      return List.of(OFFLINE_ONLY_TABLE, REALTIME_ONLY_TABLE, HYBRID_TABLE, LOGICAL_TABLE).contains(raw)
          ? name : null;
    };
    Mockito.when(tableCache.getActualTableName(Mockito.anyString()))
        .thenAnswer(invocation -> actualName.apply(invocation.getArgument(0)));
    Mockito.when(tableCache.isLogicalTable(Mockito.anyString()))
        .thenAnswer(invocation -> LOGICAL_TABLE.equals(invocation.getArgument(0)));
    Mockito.when(tableCache.getTableConfig(Mockito.anyString())).thenAnswer(invocation -> {
      String nameWithType = invocation.getArgument(0);
      switch (nameWithType) {
        case OFFLINE_ONLY_TABLE + "_OFFLINE":
        case REALTIME_ONLY_TABLE + "_REALTIME":
        case HYBRID_TABLE + "_OFFLINE":
        case HYBRID_TABLE + "_REALTIME":
          return tableConfig(nameWithType);
        default:
          return null;
      }
    });
    return tableCache;
  }

  private static TableConfig tableConfig(String tableNameWithType) {
    TableType type = tableNameWithType.endsWith("_REALTIME") ? TableType.REALTIME : TableType.OFFLINE;
    return new TableConfigBuilder(type).setTableName(tableNameWithType).build();
  }
}
