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

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.logical.LogicalMatch;
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
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Unit tests for [PinotMatchExchangeNodeInsertRule], which inserts the exchange below MATCH_RECOGNIZE.
///
/// The rule is driven directly instead of through a full query so that a [Match] without ORDER BY - which the
/// SQL front end rejects - can still be covered. Plan-shape assertions for real queries live in
/// `MatchRecognizeExchangePlanTest`.
public class PinotMatchExchangeNodeInsertRuleTest {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
  // (col1 VARCHAR, col2 VARCHAR, ts BIGINT)
  private static final RelDataType ROW_TYPE = TYPE_FACTORY.builder()
      .add("col1", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
      .add("col2", TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR))
      .add("ts", TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))
      .build();

  @Test
  public void testPartitionKeysArePrependedToCollation() {
    // PARTITION BY col1 ORDER BY ts
    Match match = createMatch(ImmutableBitSet.of(0), RelCollations.of(2));
    PinotLogicalSortExchange exchange = (PinotLogicalSortExchange) applyRule(match, Map.of()).getInput(0);

    assertEquals(exchange.getDistribution().getKeys(), List.of(0));
    assertEquals(exchange.getCollation().getKeys(), List.of(0, 2));
    assertFalse(exchange.isSortOnSender());
    assertTrue(exchange.isSortOnReceiver());
  }

  @Test
  public void testAllPartitionKeysArePrependedInOrder() {
    // PARTITION BY col1, col2 ORDER BY ts DESC
    Match match = createMatch(ImmutableBitSet.of(0, 1),
        RelCollations.of(new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING)));
    PinotLogicalSortExchange exchange = (PinotLogicalSortExchange) applyRule(match, Map.of()).getInput(0);

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
    PinotLogicalSortExchange exchange = (PinotLogicalSortExchange) applyRule(match, Map.of()).getInput(0);

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
    Map<String, String> options = Map.of(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY, "true");
    PinotLogicalSortExchange exchange = (PinotLogicalSortExchange) applyRule(match, options).getInput(0);

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
    // Degenerate case: nothing to sort on, so a sort exchange would be pure overhead.
    Match match = createMatch(ImmutableBitSet.of(), RelCollations.EMPTY);
    Map<String, String> options = Map.of(QueryOptionKey.ALLOW_MATCH_RECOGNIZE_WITHOUT_PARTITION_BY, "true");
    RelNode input = applyRule(match, options).getInput(0);

    assertTrue(input instanceof PinotLogicalExchange, input.getClass().getName());
    assertTrue(((PinotLogicalExchange) input).getDistribution().getKeys().isEmpty());
  }

  @Test
  public void testPartitionByWithoutOrderByStillClustersByPartition() {
    Match match = createMatch(ImmutableBitSet.of(0, 1), RelCollations.EMPTY);
    PinotLogicalSortExchange exchange = (PinotLogicalSortExchange) applyRule(match, Map.of()).getInput(0);

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
    Match matchOverExchange = (Match) match.copy(match.getTraitSet(),
        List.of(PinotLogicalExchange.create(match.getInput(), RelDistributions.hash(List.of(0)))));

    RelOptRuleCall call = mock(RelOptRuleCall.class);
    when(call.rel(0)).thenReturn(matchOverExchange);
    assertFalse(PinotMatchExchangeNodeInsertRule.INSTANCE.matches(call));
    // ... but it does fire when the input is not an exchange yet.
    when(call.rel(0)).thenReturn(match);
    assertTrue(PinotMatchExchangeNodeInsertRule.INSTANCE.matches(call));
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
    RelOptCluster cluster = RelOptCluster.create(planner, REX_BUILDER);
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
