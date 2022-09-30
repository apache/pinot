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
package org.apache.calcite.rel.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataTypeSystemImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotSortExchangeNodeInsertRuleTest {

  private final SqlTypeFactoryImpl _types = new SqlTypeFactoryImpl(RelDataTypeSystemImpl.DEFAULT);
  private final RexBuilder _rexBuilder = new RexBuilder(_types);

  @Mock
  private RelOptRuleCall _call;
  @Mock
  private RelNode _inner;
  @Mock
  private RelOptCluster _cluster;

  private AutoCloseable _mocks;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    Mockito.when(_inner.getCluster()).thenReturn(_cluster);
    Mockito.when(_inner.getTraitSet()).thenReturn(RelTraitSet.createEmpty());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldMatchSort() {
    // Given:
    PinotSortExchangeNodeInsertRule rule = PinotSortExchangeNodeInsertRule.INSTANCE;
    LogicalSort sort = LogicalSort.create(_inner, RelCollations.of(0),
        _rexBuilder.makeLiteral(1, _types.createSqlType(SqlTypeName.INTEGER)),
        _rexBuilder.makeLiteral(2, _types.createSqlType(SqlTypeName.INTEGER)));
    Mockito.when(_call.getRelList()).thenReturn(ImmutableList.of(sort));
    Mockito.when(_call.rel(0)).thenReturn(sort);

    // When:
    boolean matches = rule.matches(_call);

    // Then:
    Assert.assertTrue(matches, "Expected to match basic sort node.");
  }

  @Test
  public void shouldNotMatchSortWithInnerExchange() {
    // Given:
    PinotSortExchangeNodeInsertRule rule = PinotSortExchangeNodeInsertRule.INSTANCE;
    Exchange exchange = LogicalExchange.create(_inner, RelDistributions.SINGLETON);
    LogicalSort sort = LogicalSort.create(exchange, RelCollations.of(0),
        _rexBuilder.makeLiteral(1, _types.createSqlType(SqlTypeName.INTEGER)),
        _rexBuilder.makeLiteral(2, _types.createSqlType(SqlTypeName.INTEGER)));
    Mockito.when(_call.getRelList()).thenReturn(ImmutableList.of(sort));
    Mockito.when(_call.rel(0)).thenReturn(sort);

    // When:
    boolean matches = rule.matches(_call);

    // Then:
    Assert.assertFalse(matches, "Should not match sort node that comes after an Exchange.");
  }

  @Test
  public void shouldNotMatchInnerSort() {
    // Given:
    PinotSortExchangeNodeInsertRule rule = PinotSortExchangeNodeInsertRule.INSTANCE;
    InnerSort sort = InnerSort.create(_inner, RelCollations.of(0),
        _rexBuilder.makeLiteral(1, _types.createSqlType(SqlTypeName.INTEGER)),
        _rexBuilder.makeLiteral(2, _types.createSqlType(SqlTypeName.INTEGER)));
    Mockito.when(_call.getRelList()).thenReturn(ImmutableList.of(sort));
    Mockito.when(_call.rel(0)).thenReturn(sort);

    // When:
    boolean matches = rule.matches(_call);

    // Then:
    Assert.assertFalse(matches, "Should not match inner sort node.");
  }

  @Test
  public void shouldCreateSortBeforeAndAfterExchangeWhenMatches() {
    // Given:
    PinotSortExchangeNodeInsertRule rule = PinotSortExchangeNodeInsertRule.INSTANCE;
    Exchange exchange = LogicalExchange.create(_inner, RelDistributions.SINGLETON);
    LogicalSort sort = LogicalSort.create(exchange, RelCollations.of(0),
        _rexBuilder.makeLiteral(1, _types.createSqlType(SqlTypeName.INTEGER)),
        _rexBuilder.makeLiteral(2, _types.createSqlType(SqlTypeName.INTEGER)));
    Mockito.when(_call.getRelList()).thenReturn(ImmutableList.of(sort));
    Mockito.when(_call.rel(0)).thenReturn(sort);

    // When:
    ArgumentCaptor<RelNode> nodeCaptor = ArgumentCaptor.forClass(RelNode.class);
    rule.onMatch(_call);

    // Then:
    Mockito.verify(_call).transformTo(nodeCaptor.capture(), Mockito.anyMap());
    RelNode capture = nodeCaptor.getValue();

    Assert.assertTrue(capture instanceof LogicalSort);
    Assert.assertTrue(((LogicalSort) capture).getInput() instanceof LogicalExchange);
    Assert.assertTrue(((LogicalExchange) ((LogicalSort) capture).getInput()).getInput() instanceof InnerSort);
  }
}
