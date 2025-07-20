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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlAvgAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.pinot.query.type.TypeFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PinotWindowSplitRuleTest {
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
  public void testMatchesWithSingleGroup() {
    // Test that the rule doesn't match when there's only one window group
    LogicalWindow singleGroupWindow = createWindowWithGroups(1);
    Mockito.when(_call.rel(0)).thenReturn(singleGroupWindow);

    boolean matches = PinotWindowSplitRule.INSTANCE.matches(_call);
    Assert.assertFalse(matches, "Rule should not match when there's only one window group");
  }

  @Test
  public void testMatchesWithMultipleGroups() {
    // Test that the rule matches when there are multiple window groups
    LogicalWindow multiGroupWindow = createWindowWithGroups(3);
    Mockito.when(_call.rel(0)).thenReturn(multiGroupWindow);

    boolean matches = PinotWindowSplitRule.INSTANCE.matches(_call);
    Assert.assertTrue(matches, "Rule should match when there are multiple window groups");
  }

  @Test
  public void testOnMatchWithTwoGroups() {
    // Test splitting a window with two groups into a chain of two windows
    LogicalWindow originalWindow = createTestWindow(2);
    Mockito.when(_call.rel(0)).thenReturn(originalWindow);

    PinotWindowSplitRule.INSTANCE.onMatch(_call);

    // Verify that transformTo was called with a LogicalWindow
    ArgumentCaptor<RelNode> transformedNodeCapture = ArgumentCaptor.forClass(RelNode.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(transformedNodeCapture.capture());

    RelNode transformedNode = transformedNodeCapture.getValue();
    Assert.assertTrue(transformedNode instanceof LogicalWindow,
        "Transformed node should be a LogicalWindow");

    LogicalWindow resultWindow = (LogicalWindow) transformedNode;
    Assert.assertEquals(resultWindow.groups.size(), 1,
        "Final window should have exactly one group");
  }

  @Test
  public void testOnMatchWithThreeGroups() {
    // Test splitting a window with three groups into a chain of three windows
    LogicalWindow originalWindow = createTestWindow(3);
    Mockito.when(_call.rel(0)).thenReturn(originalWindow);

    PinotWindowSplitRule.INSTANCE.onMatch(_call);

    // Verify that transformTo was called with a LogicalWindow
    ArgumentCaptor<RelNode> transformedNodeCapture = ArgumentCaptor.forClass(RelNode.class);
    Mockito.verify(_call, Mockito.times(1)).transformTo(transformedNodeCapture.capture());

    RelNode transformedNode = transformedNodeCapture.getValue();
    Assert.assertTrue(transformedNode instanceof LogicalWindow,
        "Transformed node should be a LogicalWindow");

    LogicalWindow resultWindow = (LogicalWindow) transformedNode;
    Assert.assertEquals(resultWindow.groups.size(), 1,
        "Final window should have exactly one group");
  }

  @Test
  public void testRexConstantRefShifter() {
    // Test the RexConstantRefShifter functionality
    int originalInputFieldCount = 2;
    int shift = 1;

    // Create a RexInputRef that points to a constant (index >= originalInputFieldCount)
    RexInputRef constantRef = new RexInputRef(3, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

    // Create a RexInputRef that points to an input field (index < originalInputFieldCount)
    RexInputRef inputFieldRef = new RexInputRef(1, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

    // Create a window group with these references
    Window.Group group = new Window.Group(
        ImmutableBitSet.of(0),
        true,
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.UNBOUNDED_FOLLOWING,
        RexWindowExclusion.EXCLUDE_NO_OTHER,
        RelCollations.EMPTY,
        Collections.emptyList()
    );

    // Apply the shifter
    PinotWindowSplitRule.RexConstantRefShifter shifter =
        new PinotWindowSplitRule.RexConstantRefShifter(originalInputFieldCount, shift);
    Window.Group shiftedGroup = shifter.apply(group);

    // The group should be the same since it doesn't contain the RexInputRefs we're testing
    Assert.assertEquals(shiftedGroup, group);
  }

  @Test
  public void testRexConstantRefShifterWithAggCalls() {
    // Test the RexConstantRefShifter with window aggregate calls
    int originalInputFieldCount = 2;
    int shift = 1;

    // Create RexInputRefs for testing
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexInputRef inputFieldRef = new RexInputRef(0, intType); // Input field
    RexInputRef constantRef = new RexInputRef(3, intType);   // Constant

    // Create window aggregate calls
    Window.RexWinAggCall inputFieldAgg = new Window.RexWinAggCall(
        new SqlSumAggFunction(intType), intType, List.of(inputFieldRef), 0, false, false);
    Window.RexWinAggCall constantAgg = new Window.RexWinAggCall(
        new SqlAvgAggFunction(SqlKind.AVG), intType, List.of(constantRef), 1, false, false);

    // Create a window group with these aggregate calls
    Window.Group group = new Window.Group(
        ImmutableBitSet.of(0),
        true,
        RexWindowBounds.UNBOUNDED_PRECEDING,
        RexWindowBounds.UNBOUNDED_FOLLOWING,
        RexWindowExclusion.EXCLUDE_NO_OTHER,
        RelCollations.EMPTY,
        Arrays.asList(inputFieldAgg, constantAgg)
    );

    // Apply the shifter
    PinotWindowSplitRule.RexConstantRefShifter shifter =
        new PinotWindowSplitRule.RexConstantRefShifter(originalInputFieldCount, shift);
    Window.Group shiftedGroup = shifter.apply(group);

    // Verify that the input field reference wasn't shifted
    Window.RexWinAggCall shiftedInputFieldAgg = shiftedGroup.aggCalls.get(0);
    RexInputRef shiftedInputFieldRef = (RexInputRef) shiftedInputFieldAgg.getOperands().get(0);
    Assert.assertEquals(shiftedInputFieldRef.getIndex(), 0,
        "Input field reference should not be shifted");

    // Verify that the constant reference was shifted
    Window.RexWinAggCall shiftedConstantAgg = shiftedGroup.aggCalls.get(1);
    RexInputRef shiftedConstantRef = (RexInputRef) shiftedConstantAgg.getOperands().get(0);
    Assert.assertEquals(shiftedConstantRef.getIndex(), 4,
        "Constant reference should be shifted by " + shift);
  }

  private LogicalWindow createWindowWithGroups(int numGroups) {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexInputRef inputRef = new RexInputRef(0, intType);

    List<Window.Group> groups = createWindowGroups(numGroups);

    List<RexLiteral> constants = Collections.emptyList();
    return LogicalWindow.create(RelTraitSet.createEmpty(), _input, constants, intType, groups);
  }

  private List<Window.Group> createWindowGroups(int numGroups) {
    RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexInputRef inputRef = new RexInputRef(0, intType);

    List<Window.Group> groups = new ArrayList<>();
    for (int i = 0; i < numGroups; i++) {
      Window.RexWinAggCall aggCall = new Window.RexWinAggCall(
          new SqlSumAggFunction(intType), intType, List.of(inputRef), i, false, false);
      Window.Group group = new Window.Group(
          ImmutableBitSet.of(0),
          true,
          RexWindowBounds.UNBOUNDED_PRECEDING,
          RexWindowBounds.UNBOUNDED_FOLLOWING,
          RexWindowExclusion.EXCLUDE_NO_OTHER,
          RelCollations.EMPTY,
          List.of(aggCall)
      );
      groups.add(group);
    }
    return groups;
  }

  private LogicalWindow createTestWindow(int numGroups) {
    final RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RexInputRef inputRef = new RexInputRef(0, intType);

    // Create a real RelOptCluster
    HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
    RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(TYPE_FACTORY));

    // Create a real input project with real row type
    final List<RelDataTypeField> inputFields = createInputFields(1);
    RelDataType inputRowType = TYPE_FACTORY.createStructType(inputFields);
    LogicalProject inputProject = LogicalProject.create(
        LogicalValues.create(cluster, inputRowType, ImmutableList.of()),
        Collections.emptyList(),
        List.of(inputRef),
        inputRowType
    );
    Mockito.when(_input.getCurrentRel()).thenReturn(inputProject);
    Mockito.when(_input.getCluster()).thenReturn(cluster);

    // Create real window groups
    List<Window.Group> groups = createWindowGroups(numGroups);
    List<RexLiteral> constants = Collections.emptyList();

    // Create real struct row type for the window
    List<RelDataTypeField> windowFields = new ArrayList<>(inputFields);
    for (int i = 0; i < groups.size(); i++) {
      final int aggIdx = i;
      windowFields.add(new RelDataTypeFieldImpl(
        "agg_field" + aggIdx,
        inputFields.size() + aggIdx,
        intType
      ));
    }
    RelDataType windowRowType = TYPE_FACTORY.createStructType(windowFields);

    return LogicalWindow.create(
        RelTraitSet.createEmpty(), inputProject, constants, windowRowType, groups);
  }

  private List<RelDataTypeField> createInputFields(int count) {
    List<RelDataTypeField> fields = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      fields.add(new RelDataTypeFieldImpl(
        "field" + i,
        i,
        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)
      ));
    }
    return fields;
  }
}
