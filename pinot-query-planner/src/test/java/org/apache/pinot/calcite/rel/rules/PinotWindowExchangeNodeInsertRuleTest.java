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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.query.planner.logical.RexExpressionUtils;
import org.apache.pinot.query.type.TypeFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PinotWindowExchangeNodeInsertRuleTest {
  private static final TypeFactory TYPE_FACTORY = new TypeFactory();
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

  private AutoCloseable _mocks;

  @Mock
  private RelOptRuleCall _call;
  @Mock
  private HepRelVertex _input;
  @Mock
  private RelOptCluster _cluster;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
    RelTraitSet traits = RelTraitSet.createEmpty();
    Mockito.when(_input.getTraitSet()).thenReturn(traits);
    Mockito.when(_input.getCluster()).thenReturn(_cluster);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testLiteralWindowOffsetBoundsExtraction() {
    // Create a RelOptRuleCall for a window with offset lower and upper bounds and test the extraction of the bounds
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RexInputRef windowFunctionInputRef = Mockito.mock(RexInputRef.class);
    Mockito.when(windowFunctionInputRef.getIndex()).thenReturn(0);
    Mockito.when(windowFunctionInputRef.getType()).thenReturn(intType);
    RexInputRef partitionColInputRef = Mockito.mock(RexInputRef.class);
    Mockito.when(partitionColInputRef.getIndex()).thenReturn(1);
    Mockito.when(partitionColInputRef.getType()).thenReturn(intType);
    RexInputRef orderColInputRef = Mockito.mock(RexInputRef.class);
    Mockito.when(orderColInputRef.getIndex()).thenReturn(2);
    Mockito.when(orderColInputRef.getType()).thenReturn(intType);
    RexInputRef lowerBoundInputRef = Mockito.mock(RexInputRef.class);
    Mockito.when(lowerBoundInputRef.getIndex()).thenReturn(3);
    Mockito.when(lowerBoundInputRef.getType()).thenReturn(intType);
    RexInputRef upperBoundInputRef = Mockito.mock(RexInputRef.class);
    Mockito.when(upperBoundInputRef.getIndex()).thenReturn(4);
    Mockito.when(upperBoundInputRef.getType()).thenReturn(intType);

    List<Window.Group> groups = Collections.singletonList(
        new Window.Group(ImmutableBitSet.of(List.of(1)), true, RexWindowBounds.preceding(lowerBoundInputRef),
            RexWindowBounds.following(upperBoundInputRef), RelCollations.of(2), List.of(
            new Window.RexWinAggCall(new SqlSumAggFunction(intType), intType, List.of(windowFunctionInputRef), 0, false,
                false))));

    List<RexLiteral> literals = List.of(REX_BUILDER.makeLiteral(5, intType), REX_BUILDER.makeLiteral(10, intType));
    LogicalWindow originalWindow = LogicalWindow.create(RelTraitSet.createEmpty(), _input, literals, intType, groups);

    LogicalProject inputProject = Mockito.mock(LogicalProject.class);
    RelDataType projectDataType = Mockito.mock(RelDataType.class);
    Mockito.when(projectDataType.getFieldCount()).thenReturn(3);
    Mockito.when(inputProject.getRowType()).thenReturn(projectDataType);
    Mockito.when(inputProject.getProjects())
        .thenReturn(List.of(windowFunctionInputRef, partitionColInputRef, orderColInputRef));
    Mockito.when(_input.getCurrentRel()).thenReturn(inputProject);
    Mockito.when(_call.rel(0)).thenReturn(originalWindow);
    PinotWindowExchangeNodeInsertRule.INSTANCE.onMatch(_call);

    ArgumentCaptor<RelNode> logicalWindowCapture = ArgumentCaptor.forClass(LogicalWindow.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(logicalWindowCapture.capture());
    LogicalWindow newWindow = (LogicalWindow) logicalWindowCapture.getValue();
    Assert.assertEquals(RexExpressionUtils.getValueAsInt(newWindow.groups.get(0).lowerBound.getOffset()), 5);
    Assert.assertTrue(newWindow.groups.get(0).lowerBound.isPreceding());
    Assert.assertEquals(RexExpressionUtils.getValueAsInt(newWindow.groups.get(0).upperBound.getOffset()), 10);
    Assert.assertTrue(newWindow.groups.get(0).upperBound.isFollowing());
  }
}
