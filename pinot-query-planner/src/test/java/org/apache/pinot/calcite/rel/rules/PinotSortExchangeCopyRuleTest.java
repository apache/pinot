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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.SortExchange;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.calcite.rel.logical.PinotLogicalSortExchange;
import org.apache.pinot.query.type.TypeFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotSortExchangeCopyRuleTest {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private AutoCloseable _mocks;

  @Mock
  private RelOptRuleCall _call;
  @Mock
  private RelNode _input;
  @Mock
  private RelOptCluster _cluster;
  @Mock
  private RelMetadataQuery _query;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    RelTraitSet traits = RelTraitSet.createEmpty();
    Mockito.when(_input.getTraitSet()).thenReturn(traits);
    Mockito.when(_input.getCluster()).thenReturn(_cluster);
    Mockito.when(_call.getMetadataQuery()).thenReturn(_query);
    Mockito.when(_query.getMaxRowCount(Mockito.any())).thenReturn(null);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldMatchLimitNoOffsetNoSort() {
    // Given:
    SortExchange exchange =
        PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, RelCollations.EMPTY, false, false);
    Sort sort = LogicalSort.create(exchange, RelCollations.EMPTY, null, literal(1));
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(sortCopyCapture.capture());

    RelNode sortCopy = sortCopyCapture.getValue();
    Assert.assertTrue(sortCopy instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput() instanceof PinotLogicalSortExchange);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput().getInput(0) instanceof LogicalSort);

    LogicalSort innerSort = (LogicalSort) ((LogicalSort) sortCopy).getInput().getInput(0);
    Assert.assertEquals(innerSort.getCollation().getKeys().size(), 0);
    Assert.assertNull((innerSort).offset);
    Assert.assertEquals((innerSort).fetch, literal(1));
  }

  @Test
  public void shouldMatchLimitNoOffsetYesSortNoSortEnabled() {
    // Given:
    RelCollation collation = RelCollations.of(1);
    SortExchange exchange =
        PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, collation, false, false);
    Sort sort = LogicalSort.create(exchange, collation, null, literal(1));
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(sortCopyCapture.capture());

    RelNode sortCopy = sortCopyCapture.getValue();
    Assert.assertTrue(sortCopy instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput() instanceof PinotLogicalSortExchange);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput().getInput(0) instanceof LogicalSort);

    LogicalSort innerSort = (LogicalSort) ((LogicalSort) sortCopy).getInput().getInput(0);
    Assert.assertEquals(innerSort.getCollation().getKeys().size(), 1);
    Assert.assertNull((innerSort).offset);
    Assert.assertEquals((innerSort).fetch, literal(1));
  }

  @Test
  public void shouldMatchLimitNoOffsetYesSortOnSender() {
    // Given:
    RelCollation collation = RelCollations.of(1);
    SortExchange exchange = PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, collation, true, false);
    Sort sort = LogicalSort.create(exchange, collation, null, literal(1));
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(sortCopyCapture.capture());

    RelNode sortCopy = sortCopyCapture.getValue();
    Assert.assertTrue(sortCopy instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput() instanceof PinotLogicalSortExchange);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput().getInput(0) instanceof LogicalSort);

    LogicalSort innerSort = (LogicalSort) ((LogicalSort) sortCopy).getInput().getInput(0);
    Assert.assertEquals(innerSort.getCollation().getKeys().size(), 1);
    Assert.assertNull((innerSort).offset);
    Assert.assertEquals((innerSort).fetch, literal(1));
  }

  @Test
  public void shouldMatchLimitNoOffsetYesSort() {
    // Given:
    RelCollation collation = RelCollations.of(1);
    SortExchange exchange = PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, collation, false, true);
    Sort sort = LogicalSort.create(exchange, collation, null, literal(1));
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(sortCopyCapture.capture());

    RelNode sortCopy = sortCopyCapture.getValue();
    Assert.assertTrue(sortCopy instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput() instanceof PinotLogicalSortExchange);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput().getInput(0) instanceof LogicalSort);

    LogicalSort innerSort = (LogicalSort) ((LogicalSort) sortCopy).getInput().getInput(0);
    Assert.assertEquals(innerSort.getCollation(), collation);
    Assert.assertNull((innerSort).offset);
    Assert.assertEquals((innerSort).fetch, literal(1));
  }

  @Test
  public void shouldMatchNoSortAndPushDownLimitPlusOffset() {
    // Given:
    SortExchange exchange =
        PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, RelCollations.EMPTY, false, true);
    Sort sort = LogicalSort.create(exchange, RelCollations.EMPTY, literal(2), literal(1));
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(sortCopyCapture.capture());

    RelNode sortCopy = sortCopyCapture.getValue();
    Assert.assertTrue(sortCopy instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput() instanceof PinotLogicalSortExchange);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput().getInput(0) instanceof LogicalSort);

    LogicalSort innerSort = (LogicalSort) ((LogicalSort) sortCopy).getInput().getInput(0);
    Assert.assertEquals(innerSort.getCollation().getKeys().size(), 0);
    Assert.assertNull((innerSort).offset);
    Assert.assertEquals((innerSort).fetch, literal(3));
  }

  @Test
  public void shouldMatchSortOnly() {
    // Given:
    RelCollation collation = RelCollations.of(1);
    SortExchange exchange = PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, collation, false, true);
    Sort sort = LogicalSort.create(exchange, collation, null, null);
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.never()).transformTo(sortCopyCapture.capture());
  }

  @Test
  public void shouldMatchLimitOffsetAndSort() {
    // Given:
    RelCollation collation = RelCollations.of(1);
    SortExchange exchange = PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, collation, false, true);
    Sort sort = LogicalSort.create(exchange, collation, literal(1), literal(2));
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    ArgumentCaptor<RelNode> sortCopyCapture = ArgumentCaptor.forClass(LogicalSort.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(sortCopyCapture.capture());

    RelNode sortCopy = sortCopyCapture.getValue();
    Assert.assertTrue(sortCopy instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput() instanceof PinotLogicalSortExchange);
    Assert.assertTrue(((LogicalSort) sortCopy).getInput().getInput(0) instanceof LogicalSort);

    LogicalSort innerSort = (LogicalSort) ((LogicalSort) sortCopy).getInput().getInput(0);
    Assert.assertEquals(innerSort.getCollation(), collation);
    Assert.assertNull((innerSort).offset);
    Assert.assertEquals((innerSort).fetch, literal(3));
  }

  @Test
  public void shouldNotMatchOnlySortAlreadySorted() {
    // Given:
    RelCollation collation = RelCollations.of(1);
    SortExchange exchange = PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, collation, false, true);
    Sort sort = LogicalSort.create(exchange, collation, null, null);
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);
    Mockito.when(_query.collations(_input)).thenReturn(ImmutableList.of(collation));

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    Mockito.verify(_call, Mockito.never()).transformTo(Mockito.any(), Mockito.anyMap());
  }

  @Test
  public void shouldNotMatchOffsetNoLimitNoSort() {
    // Given:
    SortExchange exchange =
        PinotLogicalSortExchange.create(_input, RelDistributions.SINGLETON, RelCollations.EMPTY, false, true);
    Sort sort = LogicalSort.create(exchange, RelCollations.EMPTY, literal(1), null);
    Mockito.when(_call.rel(0)).thenReturn(sort);
    Mockito.when(_call.rel(1)).thenReturn(exchange);

    // When:
    PinotSortExchangeCopyRule.SORT_EXCHANGE_COPY.onMatch(_call);

    // Then:
    Mockito.verify(_call, Mockito.never()).transformTo(Mockito.any(), Mockito.anyMap());
  }

  private static RexNode literal(int i) {
    return REX_BUILDER.makeLiteral(i, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));
  }
}
