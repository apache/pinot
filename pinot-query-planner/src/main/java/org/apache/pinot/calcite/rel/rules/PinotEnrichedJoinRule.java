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
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.immutables.value.Value;

@Value.Enclosing
public class PinotEnrichedJoinRule extends RelRule<RelRule.Config> {
  enum PatternType {
    SORT_PROJECT_FILTER_JOIN,
    SORT_PROJECT_JOIN,
    SORT_FILTER_JOIN,
    PROJECT_FILTER_JOIN,
    SORT_JOIN,
    PROJECT_JOIN,
    FILTER_JOIN
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    @Override
    default PinotEnrichedJoinRule toRule() {
      return new PinotEnrichedJoinRule(this);
    }

    PatternType patternType();
    Config withPatternType(PatternType patternType);

    Config SORT_PROJECT_FILTER_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(LogicalProject.class).oneInput(b2 ->
                    b2.operand(LogicalFilter.class).oneInput(b3 ->
                        b3.operand(LogicalJoin.class).anyInputs()
                    )
                )
            )
        ).patternType(PatternType.SORT_PROJECT_FILTER_JOIN).build();

    Config SORT_PROJECT_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(LogicalProject.class).oneInput(b2 ->
                    b2.operand(LogicalJoin.class).anyInputs()
                )
            )
        ).patternType(PatternType.SORT_PROJECT_JOIN).build();

    Config SORT_FILTER_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(LogicalFilter.class).oneInput(b2 ->
                    b2.operand(LogicalJoin.class).anyInputs()
                )
            )
        ).patternType(PatternType.SORT_FILTER_JOIN).build();

    Config PROJECT_FILTER_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(LogicalFilter.class).oneInput(b2 ->
                    b2.operand(LogicalJoin.class).anyInputs()
                )
            )
        ).patternType(PatternType.PROJECT_FILTER_JOIN).build();

    Config SORT_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(LogicalJoin.class).anyInputs()
            )
        ).patternType(PatternType.SORT_JOIN).build();

    Config PROJECT_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(LogicalJoin.class).anyInputs()
            )
        ).patternType(PatternType.PROJECT_JOIN).build();

    Config FILTER_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalFilter.class).oneInput(b1 ->
                b1.operand(LogicalJoin.class).anyInputs()
            )
        ).patternType(PatternType.FILTER_JOIN).build();
  }

  private PinotEnrichedJoinRule(Config config) {
    super(config);
  }

  public static final PinotEnrichedJoinRule SORT_PROJECT_FILTER_JOIN =
      Config.SORT_PROJECT_FILTER_JOIN.toRule();
  public static final PinotEnrichedJoinRule SORT_PROJECT_JOIN =
      Config.SORT_PROJECT_JOIN.toRule();
  public static final PinotEnrichedJoinRule SORT_FILTER_JOIN =
      Config.SORT_FILTER_JOIN.toRule();
  public static final PinotEnrichedJoinRule PROJECT_FILTER_JOIN =
      Config.PROJECT_FILTER_JOIN.toRule();
  public static final PinotEnrichedJoinRule SORT_JOIN =
      Config.SORT_JOIN.toRule();
  public static final PinotEnrichedJoinRule PROJECT_JOIN =
      Config.PROJECT_JOIN.toRule();
  public static final PinotEnrichedJoinRule FILTER_JOIN =
      Config.FILTER_JOIN.toRule();

  public static final List<RelOptRule> PINOT_ENRICHED_JOIN_RULES = List.of(
      SORT_PROJECT_FILTER_JOIN,
      SORT_PROJECT_JOIN,
      SORT_FILTER_JOIN,
      PROJECT_FILTER_JOIN,
      SORT_JOIN,
      PROJECT_JOIN,
      FILTER_JOIN
  );

  @Override
  public void onMatch(RelOptRuleCall call) {
    Preconditions.checkState(call.getRule() instanceof PinotEnrichedJoinRule);
    PatternType patternType = ((RelRule<Config>) call.getRule()).config.patternType();

    RelTraitSet traitSet;
    @Nullable final RexNode filterRex;
    @Nullable final List<RexNode> projects;
    @Nullable final RelDataType projectRowType;
    @Nullable final Set<CorrelationId> projectVariableSet;
    @Nullable final RelCollation collation;
    @Nullable final RexNode fetch;
    @Nullable final RexNode offset;
    @Nullable final LogicalSort sort;
    @Nullable final LogicalProject project;
    @Nullable final LogicalFilter filter;
    final LogicalJoin join;

    switch (patternType) {
      case SORT_PROJECT_FILTER_JOIN:
        sort = call.rel(0);
        project = call.rel(1);
        filter = call.rel(2);
        join = call.rel(3);

        traitSet = sort.getTraitSet();
        filterRex = filter.getCondition();
        projects = project.getProjects();
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();
        collation = sort.getCollation();
        fetch = sort.fetch;
        offset = sort.offset;
        break;

      case SORT_PROJECT_JOIN:
        sort = call.rel(0);
        project = call.rel(1);
        join = call.rel(2);

        traitSet = sort.getTraitSet();
        filterRex = null;
        projects = project.getProjects();
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();
        collation = sort.getCollation();
        fetch = sort.fetch;
        offset = sort.offset;
        break;

      case SORT_FILTER_JOIN:
        sort = call.rel(0);
        filter = call.rel(1);
        join = call.rel(2);

        traitSet = sort.getTraitSet();
        filterRex = filter.getCondition();
        projects = null;
        projectRowType = null;
        projectVariableSet = null;
        collation = sort.getCollation();
        fetch = sort.fetch;
        offset = sort.offset;
        break;

      case PROJECT_FILTER_JOIN:
        project = call.rel(0);
        filter = call.rel(1);
        join = call.rel(2);

        traitSet = join.getTraitSet();
        filterRex = filter.getCondition();
        projects = project.getProjects();
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();
        collation = null;
        fetch = null;
        offset = null;
        break;

      case SORT_JOIN:
        sort = call.rel(0);
        join = call.rel(1);

        traitSet = sort.getTraitSet();
        filterRex = null;
        projects = null;
        projectRowType = null;
        projectVariableSet = null;
        collation = sort.getCollation();
        fetch = sort.fetch;
        offset = sort.offset;
        break;

      case PROJECT_JOIN:
        project = call.rel(0);
        join = call.rel(1);

        traitSet = join.getTraitSet();
        filterRex = null;
        projects = project.getProjects();
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();
        collation = null;
        fetch = null;
        offset = null;
        break;

      case FILTER_JOIN:
        filter = call.rel(0);
        join = call.rel(1);

        traitSet = join.getTraitSet();
        filterRex = filter.getCondition();
        projects = null;
        projectRowType = null;
        projectVariableSet = null;
        collation = null;
        fetch = null;
        offset = null;
        break;

      default:
        throw new IllegalStateException("unknown patternType for PinotEnrichedJoinRule");
    }

    PinotLogicalEnrichedJoin pinotLogicalEnrichedJoin = new PinotLogicalEnrichedJoin(
        join.getCluster(),
        traitSet,
        join.getHints(), join.getInput(0), join.getInput(1),
        join.getCondition(), join.getVariablesSet(), join.getJoinType(),
        filterRex,
        projects, projectRowType, projectVariableSet,
        collation, fetch, offset);

    call.transformTo(pinotLogicalEnrichedJoin);
  }

}
