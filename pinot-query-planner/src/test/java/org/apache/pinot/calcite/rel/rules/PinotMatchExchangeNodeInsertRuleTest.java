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
package org.apache.pinot.calcite.rel.rules;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.calcite.rel.logical.PinotLogicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for [PinotMatchExchangeNodeInsertRule], which inserts the exchange and the Sort below MATCH_RECOGNIZE.
///
/// The rule is driven directly instead of through a full query so that a [Match] without ORDER BY - which the
/// SQL front end rejects - can still be covered. Plan-shape assertions for real queries live in
/// `queries/MatchRecognizePlans.json`, which `ResourceBasedQueryPlansTest` runs.
public class PinotMatchExchangeNodeInsertRuleTest {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  // (col1 VARCHAR, col2 VARCHAR, ts BIGINT)
  private static final RelDataType ROW_TYPE = TYPE_FACTORY.builder()
      .add("col1", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
      .add("col2", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
      .add("ts", TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))
      .build();
  private static final Map<String, String> ALLOW_WITHOUT_PARTITION_BY =
      Map.of(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY, "true");

  @Test
  public void testPartitionKeysArePrependedToCollation() {
    // PARTITION BY col1 ORDER BY ts
    Match match = createMatch(ImmutableBitSet.of(0), RelCollations.of(2));
    PinotLogicalSortExchange exchange = sortedExchangeBelow(applyRule(match, Map.of()));

    assertEquals(exchange.getDistribution().getKeys(), List.of(0));
    assertEquals(exchange.getCollation().getKeys(), List.of(0, 2));
  }

  @Test
  public void testAllPartitionKeysArePrependedInOrder() {
    // PARTITION BY col1, col2 ORDER BY ts DESC
    Match match = createMatch(ImmutableBitSet.of(0, 1),
        RelCollations.of(new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING)));
    PinotLogicalSortExchange exchange = sortedExchangeBelow(applyRule(match, Map.of()));

    assertEquals(exchange.getDistribution().getType(), RelDistribution.Type.HASH_DISTRIBUTED);
    assertEquals(exchange.getDistribution().getKeys(), List.of(0, 1));
    List<RelFieldCollation> fieldCollations = exchange.getCollation().getFieldCollations();
    assertEquals(fieldCollations.size(), 3);
    assertEquals(fieldCollations.get(0).getFieldIndex(), 0);
    assertEquals(fieldCollations.get(0).getDirection(), RelFieldCollation.Direction.ASCENDING);
    assertEquals(fieldCollations.get(1).getFieldIndex(), 1);
    assertEquals(fieldCollations.get(1).getDirection(), RelFieldCollation.Direction.ASCENDING);
    // The ORDER BY key keeps its own direction.
    assertEquals(fieldCollations.get(2).getFieldIndex(), 2);
    assertEquals(fieldCollations.get(2).getDirection(), RelFieldCollation.Direction.DESCENDING);
  }

  @Test
  public void testOrderKeyThatIsAlsoPartitionKeyIsNotRepeated() {
    // PARTITION BY col1 ORDER BY col1 DESC, ts: col1 is constant within a partition, so it must not appear twice.
    Match match = createMatch(ImmutableBitSet.of(0),
        RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING),
            new RelFieldCollation(2)));
    PinotLogicalSortExchange exchange = sortedExchangeBelow(applyRule(match, Map.of()));

    assertEquals(exchange.getCollation().getKeys(), List.of(0, 2));
    assertEquals(exchange.getCollation().getFieldCollations().get(0).getDirection(),
        RelFieldCollation.Direction.ASCENDING);
  }

  @Test
  public void testMissingPartitionByIsRejected() {
    Match match = createMatch(ImmutableBitSet.of(), RelCollations.of(2));

    QueryException exception = expectThrows(QueryException.class, () -> applyRule(match, Map.of()));
    assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_PLANNING);
    assertTrue(exception.getMessage().contains("MATCH_RECOGNIZE without a PARTITION BY clause is not allowed"),
        exception.getMessage());
    // The message has to name the escape hatch, otherwise the user cannot act on it.
    assertTrue(exception.getMessage().contains(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY),
        exception.getMessage());
  }

  @Test
  public void testMissingPartitionByIsAllowedByQueryOption() {
    Match match = createMatch(ImmutableBitSet.of(), RelCollations.of(2));
    PinotLogicalSortExchange exchange = sortedExchangeBelow(applyRule(match, ALLOW_WITHOUT_PARTITION_BY));

    // Hashing on zero keys funnels every row to a single worker, which is exactly what the option opts into.
    assertTrue(exchange.getDistribution().getKeys().isEmpty());
    assertEquals(exchange.getCollation().getKeys(), List.of(2));
  }

  @Test
  public void testQueryOptionValueOtherThanTrueStillRejects() {
    Match match = createMatch(ImmutableBitSet.of(), RelCollations.of(2));
    Map<String, String> options = Map.of(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY, "false");

    expectThrows(QueryException.class, () -> applyRule(match, options));
  }

  @Test
  public void testNoPartitionByAndNoOrderByUsesPlainExchange() {
    // Degenerate case: nothing to sort on, so a Sort or a sort exchange would be pure overhead.
    Match match = createMatch(ImmutableBitSet.of(), RelCollations.EMPTY);
    RelNode input = applyRule(match, ALLOW_WITHOUT_PARTITION_BY).getInput(0);

    assertTrue(input instanceof PinotLogicalExchange, input.getClass().getName());
    assertTrue(((PinotLogicalExchange) input).getDistribution().getKeys().isEmpty());
  }

  @Test
  public void testPartitionByWithoutOrderByStillClustersByPartition() {
    Match match = createMatch(ImmutableBitSet.of(0, 1), RelCollations.EMPTY);
    PinotLogicalSortExchange exchange = sortedExchangeBelow(applyRule(match, Map.of()));

    assertEquals(exchange.getDistribution().getKeys(), List.of(0, 1));
    assertEquals(exchange.getCollation().getKeys(), List.of(0, 1));
  }

  @Test
  public void testMatchPropertiesArePreserved() {
    Match match = createMatch(ImmutableBitSet.of(0), RelCollations.of(2));
    Match transformed = applyRule(match, Map.of());

    assertSame(transformed.getPattern(), match.getPattern());
    assertSame(transformed.getAfter(), match.getAfter());
    assertEquals(transformed.getPatternDefinitions(), match.getPatternDefinitions());
    assertEquals(transformed.getMeasures(), match.getMeasures());
    assertEquals(transformed.getPartitionKeys(), match.getPartitionKeys());
    assertEquals(transformed.getOrderKeys(), match.getOrderKeys());
    assertEquals(transformed.getRowType(), match.getRowType());
  }

  @Test
  public void testRuleDoesNotFireWhenInputIsAlreadyAnExchange() {
    Match match = createMatch(ImmutableBitSet.of(0), RelCollations.of(2));
    Match matchOverExchange =
        withInput(match, PinotLogicalExchange.create(match.getInput(), RelDistributions.hash(List.of(0))));

    assertFalse(matches(matchOverExchange));
    // ... but it does fire when the input is not an exchange yet.
    assertTrue(matches(match));
  }

  @DataProvider
  public static Object[][] processedShapes() {
    return new Object[][]{
        {ImmutableBitSet.of(0), RelCollations.of(2), Map.of()},
        {ImmutableBitSet.of(0, 1), RelCollations.EMPTY, Map.of()},
        {ImmutableBitSet.of(), RelCollations.of(2), ALLOW_WITHOUT_PARTITION_BY},
        {ImmutableBitSet.of(), RelCollations.EMPTY, ALLOW_WITHOUT_PARTITION_BY}
    };
  }

  /// The rule leaves a Sort, not an exchange, directly below the MATCH, so it has to recognize that shape as already
  /// processed. Otherwise the planner applies it again to its own output, forever.
  @Test(dataProvider = "processedShapes")
  public void testRuleDoesNotFireOnItsOwnOutput(ImmutableBitSet partitionKeys, RelCollation orderKeys,
      Map<String, String> options) {
    Match transformed = applyRule(createMatch(partitionKeys, orderKeys), options);

    assertFalse(matches(transformed), RelOptUtil.toString(transformed));
  }

  /// Runs the rule to a fixpoint the way the planner does, and checks that exactly one Sort and one exchange end up
  /// below the MATCH. The match limit only turns a rule that re-fires on its own output into a failure instead of a
  /// hang.
  @Test(dataProvider = "processedShapes")
  public void testRuleReachesFixpointInHepPlanner(ImmutableBitSet partitionKeys, RelCollation orderKeys,
      Map<String, String> options) {
    PlannerContext plannerContext = PlannerContext.forTesting(options, mock(QueryEnvironment.Config.class));
    HepProgram program = new HepProgramBuilder()
        .addMatchLimit(16)
        .addRuleInstance(PinotMatchExchangeNodeInsertRule.INSTANCE)
        .build();
    HepPlanner planner = new HepPlanner(program, plannerContext);
    planner.setRoot(createMatch(RelOptCluster.create(planner, REX_BUILDER), partitionKeys, orderKeys));
    RelNode plan = planner.findBestExp();

    String explained = RelOptUtil.toString(plan);
    assertTrue(plan instanceof Match, explained);
    assertEquals(count(plan, Exchange.class), 1, explained);
    boolean hasCollation = !partitionKeys.isEmpty() || !orderKeys.getFieldCollations().isEmpty();
    assertEquals(count(plan, Sort.class), hasCollation ? 1 : 0, explained);
  }

  /// Only the exact shape this rule produces means the MATCH is done. Any other Sort below it was placed by something
  /// else, and does not give MATCH_RECOGNIZE the distribution and the ordering it relies on, so the rule must still
  /// fire.
  @Test
  public void testRuleFiresOverUnrelatedSort() {
    Match match = createMatch(ImmutableBitSet.of(0), RelCollations.of(2));
    RelNode values = match.getInput();
    RelCollation ownCollation = RelCollations.of(new RelFieldCollation(0), new RelFieldCollation(2));
    RelNode ownExchange =
        PinotLogicalSortExchange.create(values, RelDistributions.hash(List.of(0)), ownCollation, false, false);
    RexNode ten = REX_BUILDER.makeExactLiteral(BigDecimal.TEN);

    // Sanity check: the rule's own shape is recognized.
    assertFalse(matches(withInput(match, LogicalSort.create(ownExchange, ownCollation, null, null))));

    // Same collation, but no exchange below the Sort: rows are neither hashed by partition nor gathered per worker.
    assertTrue(matches(withInput(match, LogicalSort.create(values, ownCollation, null, null))));
    // Over the right exchange, but ordered by the ORDER BY key only: partitions would interleave.
    assertTrue(matches(withInput(match, LogicalSort.create(ownExchange, RelCollations.of(2), null, null))));
    // A Sort that trims drops rows MATCH_RECOGNIZE needs.
    assertTrue(matches(withInput(match, LogicalSort.create(ownExchange, ownCollation, null, ten))));
    assertTrue(matches(withInput(match, LogicalSort.create(ownExchange, ownCollation, ten, null))));
    // The right collation, but over an exchange hashed on another column: a partition can span workers.
    RelNode otherExchange =
        PinotLogicalSortExchange.create(values, RelDistributions.hash(List.of(1)), ownCollation, false, false);
    assertTrue(matches(withInput(match, LogicalSort.create(otherExchange, ownCollation, null, null))));
  }

  /// Checks that `transformed` reads from an explicit Sort with no fetch or offset over a hash exchange that does not
  /// sort, and returns that exchange.
  private static PinotLogicalSortExchange sortedExchangeBelow(Match transformed) {
    RelNode input = transformed.getInput();
    assertTrue(input instanceof LogicalSort, "Expected a LogicalSort below the MATCH, got: " + input);
    LogicalSort sort = (LogicalSort) input;
    // No fetch or offset: the Sort keeps every row, so the runtime picks the full sort rather than a top-N.
    assertNull(sort.fetch);
    assertNull(sort.offset);
    assertTrue(sort.getInput() instanceof PinotLogicalSortExchange,
        "Expected a PinotLogicalSortExchange below the Sort, got: " + sort.getInput());
    PinotLogicalSortExchange exchange = (PinotLogicalSortExchange) sort.getInput();
    // The Sort establishes the order, so neither side of the exchange sorts. A receiver-side sort would build the
    // deprecated SortedMailboxReceiveOperator, which returns the whole worker input as a single block.
    assertFalse(exchange.isSortOnSender());
    assertFalse(exchange.isSortOnReceiver());
    assertEquals(sort.getCollation(), exchange.getCollation());
    return exchange;
  }

  private static boolean matches(Match match) {
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(match);
    return PinotMatchExchangeNodeInsertRule.INSTANCE.matches(call);
  }

  private static Match withInput(Match match, RelNode input) {
    return (Match) match.copy(match.getTraitSet(), List.of(input));
  }

  private static int count(RelNode rel, Class<? extends RelNode> type) {
    int count = type.isInstance(rel) ? 1 : 0;
    for (RelNode input : rel.getInputs()) {
      count += count(input, type);
    }
    return count;
  }

  /// Runs the rule on the given match and returns the rewritten [Match]. The planner context carries the query
  /// options so the rule can read them the same way it does during real planning.
  private static Match applyRule(Match match, Map<String, String> options) {
    PlannerContext plannerContext =
        PlannerContext.forTesting(options, mock(QueryEnvironment.Config.class));
    HepPlanner planner = new HepPlanner(new HepProgramBuilder().build(), plannerContext);
    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(match);
    when(call.getPlanner()).thenReturn(planner);

    PinotMatchExchangeNodeInsertRule.INSTANCE.onMatch(call);

    ArgumentCaptor<RelNode> captor = ArgumentCaptor.forClass(RelNode.class);
    verify(call, times(1)).transformTo(captor.capture());
    return (Match) captor.getValue();
  }

  private static Match createMatch(ImmutableBitSet partitionKeys, RelCollation orderKeys) {
    HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
    return createMatch(RelOptCluster.create(planner, REX_BUILDER), partitionKeys, orderKeys);
  }

  private static Match createMatch(RelOptCluster cluster, ImmutableBitSet partitionKeys, RelCollation orderKeys) {
    RelNode input = LogicalValues.createEmpty(cluster, ROW_TYPE);
    RexNode pattern = REX_BUILDER.makeLiteral("A");
    RexNode after = REX_BUILDER.makeFlag(SqlMatchRecognize.AfterOption.SKIP_PAST_LAST_ROW);
    Map<String, RexNode> patternDefinitions = Map.of("A", REX_BUILDER.makeLiteral(true));
    Map<String, RexNode> measures = Map.of();
    Map<String, SortedSet<String>> subsets = Map.of();
    return LogicalMatch.create(input, ROW_TYPE, pattern, false, false, patternDefinitions, measures, after, subsets,
        false, partitionKeys, orderKeys, null);
  }
}
