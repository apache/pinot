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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.arrow.util.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.immutables.value.Value;

@Value.Enclosing
public class PinotEnrichedJoinRule extends RelRule<RelRule.Config> {
  enum PatternType {
    PROJECT_FILTER_JOIN,
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

    Config PROJECT_FILTER_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(LogicalFilter.class).oneInput(b2 ->
                    b2.operand(LogicalJoin.class).anyInputs()
                )
            )
        ).patternType(PatternType.PROJECT_FILTER_JOIN).build();

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

  public static final PinotEnrichedJoinRule PROJECT_FILTER_JOIN =
      Config.PROJECT_FILTER_JOIN.toRule();
  public static final PinotEnrichedJoinRule PROJECT_JOIN =
      Config.PROJECT_JOIN.toRule();
  public static final PinotEnrichedJoinRule FILTER_JOIN =
      Config.FILTER_JOIN.toRule();

  public static final List<RelOptRule> PINOT_ENRICHED_JOIN_RULES = List.of(
      PROJECT_FILTER_JOIN,
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
    @Nullable final LogicalProject project;
    @Nullable final LogicalFilter filter;
    final LogicalJoin join;

    List<PinotLogicalEnrichedJoin.FilterProjectRexNode> filterProjectRexNodes = new ArrayList<>();

    switch (patternType) {
      case PROJECT_FILTER_JOIN:
        project = call.rel(0);
        filter = call.rel(1);
        join = call.rel(2);

        traitSet = join.getTraitSet();
        // add filter to the set
        filterRex = filter.getCondition();
        filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(filterRex));
        // add projection to the set
        projects = project.getProjects();
        filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(projects, project.getRowType()));
        // provide final output rowtype
        projectRowType = project.getRowType();

        projectVariableSet = project.getVariablesSet();
        break;

      case PROJECT_JOIN:
        project = call.rel(0);
        join = call.rel(1);

        traitSet = join.getTraitSet();
        filterRex = null;
        // add projection to the set
        projects = project.getProjects();
        filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(projects, project.getRowType()));
        // provide final output rowtype
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();
        break;

      case FILTER_JOIN:
        filter = call.rel(0);
        join = call.rel(1);

        traitSet = join.getTraitSet();
        // add filter to the set
        filterRex = filter.getCondition();
        filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(filterRex));
        // no projection, final output row type is same as join output
        projectRowType = null;
        projectVariableSet = null;
        break;

      default:
        throw new IllegalStateException("unknown patternType for PinotEnrichedJoinRule");
    }

    // Disable lookup join for now
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      return;
    }

    // Disable non-equijoin for now
    if (join.analyzeCondition().leftKeys.isEmpty()) {
      return;
    }

    PinotLogicalEnrichedJoin pinotLogicalEnrichedJoin = new PinotLogicalEnrichedJoin(
        join.getCluster(),
        traitSet,
        join.getHints(), join.getInput(0), join.getInput(1),
        join.getCondition(), join.getVariablesSet(), join.getJoinType(),
        filterProjectRexNodes,
        projectRowType,
        projectVariableSet);

    call.transformTo(pinotLogicalEnrichedJoin);
  }
}
