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
package org.apache.pinot.core.query.reduce;

import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.datatable.DataTable.MetadataKey;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.operator.blocks.results.BaseResultsBlock.EarlyTerminationReason;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Focused tests for {@link ExecutionStatsAggregator#setStatsOnMergedDataTable(DataTable)} —
 * specifically the {@link MetadataKey#EARLY_TERMINATION_REASON} round-trip for DISTINCT queries.
 *
 * <p>The aggregator collapses a single-string wire-format reason into three independent booleans
 * (one per {@link EarlyTerminationReason}); the round-trip can encode only one reason back. These
 * tests pin:
 * <ul>
 *   <li>Each of the three reasons round-trips its own boolean when it is the only one set.
 *   <li>When multiple booleans are set (different per-server reasons OR-reduced), the round-trip
 *       encodes the first set flag in declaration order. The user-visible "DISTINCT is partial"
 *       signal is preserved because any one flag independently sets partial-ness on
 *       {@link BrokerResponseNative}.
 *   <li>When no DISTINCT early-termination booleans are set, the key is absent from the merged
 *       metadata (so the downstream decoder does not see a stray non-DISTINCT reason and the
 *       merged DataTable is indistinguishable from one whose inputs all completed normally).
 * </ul>
 */
public class ExecutionStatsAggregatorTest {

  private static final ServerRoutingInstance SERVER =
      new ServerRoutingInstance("localhost", 8080, TableType.OFFLINE);

  /** Build an empty DataTable carrying a single {@code EARLY_TERMINATION_REASON} metadata entry. */
  private static DataTable serverTableWithReason(EarlyTerminationReason reason) {
    DataTable dt = DataTableBuilderFactory.getEmptyDataTable();
    dt.getMetadata().put(MetadataKey.EARLY_TERMINATION_REASON.getName(), reason.name());
    return dt;
  }

  private static DataTable emptyServerTable() {
    return DataTableBuilderFactory.getEmptyDataTable();
  }

  /**
   * Drives one aggregate-then-readback round-trip and returns the three DISTINCT booleans the
   * downstream aggregator would flip from the merged DataTable's metadata. Mirrors the production
   * sequence: aggregate per-server inputs → serialize onto merged DataTable → reduce path
   * re-aggregates the merged DataTable on a fresh aggregator.
   */
  private static boolean[] roundTrip(DataTable... inputs) {
    ExecutionStatsAggregator producer = new ExecutionStatsAggregator(false);
    for (DataTable dt : inputs) {
      producer.aggregate(SERVER, dt);
    }
    DataTable merged = DataTableBuilderFactory.getEmptyDataTable();
    producer.setStatsOnMergedDataTable(merged);

    ExecutionStatsAggregator consumer = new ExecutionStatsAggregator(false);
    consumer.aggregate(SERVER, merged);
    BrokerResponseNative response = new BrokerResponseNative();
    consumer.setStats("testTable", response, mock(BrokerMetrics.class));
    return new boolean[]{
        response.isMaxRowsInDistinctReached(),
        response.isMaxRowsWithoutChangeInDistinctReached(),
        response.isMaxExecutionTimeInDistinctReached()
    };
  }

  @Test
  public void testDistinctMaxRowsReachedRoundTrip() {
    boolean[] flags = roundTrip(serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_ROWS));
    assertTrue(flags[0], "_maxRowsInDistinctReached must round-trip");
    assertFalse(flags[1], "other DISTINCT flags must stay false");
    assertFalse(flags[2], "other DISTINCT flags must stay false");
  }

  @Test
  public void testDistinctMaxRowsWithoutChangeReachedRoundTrip() {
    boolean[] flags = roundTrip(serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_ROWS_WITHOUT_CHANGE));
    assertFalse(flags[0]);
    assertTrue(flags[1], "_maxRowsWithoutChangeInDistinctReached must round-trip");
    assertFalse(flags[2]);
  }

  @Test
  public void testDistinctMaxExecutionTimeReachedRoundTrip() {
    boolean[] flags = roundTrip(serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_EXECUTION_TIME));
    assertFalse(flags[0]);
    assertFalse(flags[1]);
    assertTrue(flags[2], "_maxExecutionTimeInDistinctReached must round-trip");
  }

  /**
   * Two inputs hit DIFFERENT DISTINCT early-termination reasons. The aggregator OR-reduces both
   * into independent booleans, but the wire format is one string per DataTable. Round-trip can
   * encode only one reason back; per the implementation contract, it picks the first set flag in
   * declaration order — {@code DISTINCT_MAX_ROWS} when both rows-reached and execution-time are
   * set.
   *
   * <p>The user-visible "DISTINCT is partial" signal is still preserved: at least one of the
   * three flags is true post-round-trip. The exact reason granularity is best-effort.
   */
  @Test
  public void testMultipleReasonsEncodeFirstInDeclarationOrder() {
    boolean[] flags = roundTrip(
        serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_EXECUTION_TIME),
        serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_ROWS));
    assertTrue(flags[0], "DISTINCT_MAX_ROWS wins over DISTINCT_MAX_EXECUTION_TIME in priority order");
    assertFalse(flags[1]);
    assertFalse(flags[2], "execution-time flag is dropped — single-string wire format can only carry one reason");
  }

  /**
   * No DISTINCT early-termination on any input. The merged DataTable must NOT carry an
   * {@code EARLY_TERMINATION_REASON} key — writing one would mislead the downstream decoder, and
   * an empty/false value is not part of the wire-format vocabulary (the consumer's
   * {@code EarlyTerminationReason.valueOf} would throw on "" or "false" and the
   * {@code IllegalArgumentException} would silently mask any other reason we tried to encode).
   */
  @Test
  public void testNoReasonProducesAbsentMetadataKey() {
    ExecutionStatsAggregator producer = new ExecutionStatsAggregator(false);
    producer.aggregate(SERVER, emptyServerTable());
    DataTable merged = DataTableBuilderFactory.getEmptyDataTable();
    producer.setStatsOnMergedDataTable(merged);

    assertNull(merged.getMetadata().get(MetadataKey.EARLY_TERMINATION_REASON.getName()),
        "no DISTINCT early-termination → key must be absent on merged DataTable");

    // Sanity: re-aggregation of the merged DataTable leaves all three flags false.
    boolean[] flags = roundTrip(emptyServerTable());
    assertFalse(flags[0]);
    assertFalse(flags[1]);
    assertFalse(flags[2]);
  }

  /**
   * Same DISTINCT reason reported by multiple inputs round-trips to the same single boolean. The
   * aggregator's OR-reduce makes this a trivial case, but pinning it catches a future regression
   * where setStatsOnMergedDataTable starts skipping the reason on repeats.
   */
  @Test
  public void testSameReasonFromMultipleInputsRoundTripsToOneBoolean() {
    boolean[] flags = roundTrip(
        serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_ROWS),
        serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_ROWS),
        serverTableWithReason(EarlyTerminationReason.DISTINCT_MAX_ROWS));
    assertTrue(flags[0]);
    assertFalse(flags[1]);
    assertFalse(flags[2]);
  }
}
