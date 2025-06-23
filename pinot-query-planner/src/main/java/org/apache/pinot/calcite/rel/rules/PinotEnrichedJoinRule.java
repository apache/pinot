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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.spi.utils.CommonConstants;
import org.immutables.value.Value;


/**
 * This rule will be fired bottom-up and fuse operators into the enriched join greedily
 */
@Value.Enclosing
public class PinotEnrichedJoinRule extends RelRule<RelRule.Config> {
  enum PatternType {
    SORT_ENRICHED_JOIN,
    PROJECT_ENRICHED_JOIN,
    FILTER_ENRICHED_JOIN,
    PROJECT_JOIN,
    FILTER_JOIN,
    SORT_JOIN
  }

  @Value.Immutable
  public interface Config extends RelRule.Config {
    @Override
    default PinotEnrichedJoinRule toRule() {
      return new PinotEnrichedJoinRule(this);
    }

    PatternType patternType();

    Config withPatternType(PatternType patternType);

    Config PROJECT_ENRICHED_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(PinotLogicalEnrichedJoin.class).anyInputs()
            )
        ).patternType(PatternType.PROJECT_ENRICHED_JOIN).build();

    Config FILTER_ENRICHED_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalFilter.class).oneInput(b1 ->
                b1.operand(PinotLogicalEnrichedJoin.class).anyInputs()
            )
        ).patternType(PatternType.FILTER_ENRICHED_JOIN).build();

    Config SORT_ENRICHED_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(PinotLogicalEnrichedJoin.class).anyInputs()
            )
        ).patternType(PatternType.SORT_ENRICHED_JOIN).build();

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

    Config SORT_JOIN = ImmutablePinotEnrichedJoinRule.Config.builder()
        .operandSupplier(b0 ->
            b0.operand(LogicalSort.class).oneInput(b1 ->
                b1.operand(LogicalJoin.class).anyInputs()
            )
        ).patternType(PatternType.SORT_JOIN).build();
  }

  private PinotEnrichedJoinRule(Config config) {
    super(config);
  }

  public static final PinotEnrichedJoinRule PROJECT_ENRICHED_JOIN =
      (PinotEnrichedJoinRule) Config.PROJECT_ENRICHED_JOIN.withDescription(
          CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN).toRule();
  public static final PinotEnrichedJoinRule FILTER_ENRICHED_JOIN =
      (PinotEnrichedJoinRule) Config.FILTER_ENRICHED_JOIN.withDescription(
          CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN).toRule();
  public static final PinotEnrichedJoinRule SORT_ENRICHED_JOIN =
      (PinotEnrichedJoinRule) Config.SORT_ENRICHED_JOIN.withDescription(
          CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN).toRule();
  public static final PinotEnrichedJoinRule PROJECT_JOIN =
      (PinotEnrichedJoinRule) Config.PROJECT_JOIN.withDescription(
          CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN).toRule();
  public static final PinotEnrichedJoinRule FILTER_JOIN =
      (PinotEnrichedJoinRule) Config.FILTER_JOIN.withDescription(
          CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN).toRule();
  public static final PinotEnrichedJoinRule SORT_JOIN =
      (PinotEnrichedJoinRule) Config.SORT_JOIN.withDescription(
          CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN).toRule();

  public static final List<RelOptRule> PINOT_ENRICHED_JOIN_RULES = List.of(
      PROJECT_ENRICHED_JOIN,
      FILTER_ENRICHED_JOIN,
      SORT_ENRICHED_JOIN,
      PROJECT_JOIN,
      FILTER_JOIN,
      SORT_JOIN
  );

  @Override
  public void onMatch(RelOptRuleCall call) {
    Preconditions.checkState(call.getRule() instanceof PinotEnrichedJoinRule);
    PatternType patternType = ((RelRule<Config>) call.getRule()).config.patternType();

    RelTraitSet traitSet;
    @Nullable
    final RexNode filterRex;
    @Nullable
    final List<RexNode> projects;
    @Nullable
    final RelDataType projectRowType;
    @Nullable
    final Set<CorrelationId> projectVariableSet;
    @Nullable
    final RelCollation collation;
    @Nullable
    final RexNode fetch;
    @Nullable
    final RexNode offset;
    @Nullable
    final LogicalProject project;
    @Nullable
    final LogicalFilter filter;
    @Nullable
    final LogicalSort sort;
    @Nullable
    PinotLogicalEnrichedJoin enrichedJoin;

    final LogicalJoin join;

    PinotLogicalEnrichedJoin newEnrichedJoin;

    List<PinotLogicalEnrichedJoin.FilterProjectRexNode> filterProjectRexNodes;

    switch (patternType) {
      // update ENRICHED_JOIN cases
      case PROJECT_ENRICHED_JOIN:
        project = call.rel(0);
        enrichedJoin = call.rel(1);

        projects = project.getProjects();
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();

        // add projection to the set
        newEnrichedJoin = enrichedJoin.withNewProject(
            new PinotLogicalEnrichedJoin.FilterProjectRexNode(projects, project.getRowType()),
            projectRowType, projectVariableSet);
        call.transformTo(newEnrichedJoin);
        return;

      case FILTER_ENRICHED_JOIN:
        filter = call.rel(0);
        enrichedJoin = call.rel(1);

        filterRex = filter.getCondition();

        // add projection to the set
        newEnrichedJoin = enrichedJoin.withNewFilter(
            new PinotLogicalEnrichedJoin.FilterProjectRexNode(filterRex));
        call.transformTo(newEnrichedJoin);
        return;

      case SORT_ENRICHED_JOIN:
        sort = call.rel(0);
        enrichedJoin = call.rel(1);

        // if enriched join already had a sort operator merged, return
        if (enrichedJoin.getOffset() != null || enrichedJoin.getFetch() != null) {
          return;
        }

        // Enriched join does not support sort collation, only fetch and offset
        collation = sort.getCollation();
        if (collation != null && !collation.equals(RelCollations.EMPTY)) {
          return;
        }

        // add sort limit to the set
        newEnrichedJoin = enrichedJoin.withNewFetchOffset(sort.fetch, sort.offset);
        call.transformTo(newEnrichedJoin);
        return;

      // JOIN TO ENRICHED_JOIN cases
      case PROJECT_JOIN:
        project = call.rel(0);
        join = call.rel(1);

        filterProjectRexNodes = new ArrayList<>();

        traitSet = join.getTraitSet();
        // add projection to the set
        projects = project.getProjects();
        filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(projects, project.getRowType()));
        // provide final output rowtype
        projectRowType = project.getRowType();
        projectVariableSet = project.getVariablesSet();

        collation = null;
        fetch = null;
        offset = null;
        break;

      case FILTER_JOIN:
        filter = call.rel(0);
        join = call.rel(1);

        filterProjectRexNodes = new ArrayList<>();

        traitSet = join.getTraitSet();
        // add filter to the set
        filterRex = filter.getCondition();
        filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(filterRex));
        // no projection, final output row type is same as join output
        projectRowType = null;
        projectVariableSet = null;

        collation = null;
        fetch = null;
        offset = null;
        break;

      case SORT_JOIN:
        sort = call.rel(0);
        join = call.rel(1);

        filterProjectRexNodes = new ArrayList<>();

        traitSet = join.getTraitSet();
        // no projection, final output row type is same as join output
        projectRowType = null;
        projectVariableSet = null;

        collation = sort.getCollation();
        fetch = sort.fetch;
        offset = sort.offset;
        break;

      default:
        throw new IllegalStateException("unknown patternType for PinotEnrichedJoinRule");
    }

    // Enriched join does not support sort collation, only fetch and offset
    if (collation != null && !collation.equals(RelCollations.EMPTY)) {
      return;
    }

    // Disable lookup join for now
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      return;
    }

    // Disable non-equijoin for now
    if (join.analyzeCondition().leftKeys.isEmpty()) {
      return;
    }

    newEnrichedJoin = new PinotLogicalEnrichedJoin(
        join.getCluster(),
        traitSet,
        join.getHints(), join.getInput(0), join.getInput(1),
        join.getCondition(), join.getVariablesSet(), join.getJoinType(),
        filterProjectRexNodes,
        projectRowType,
        projectVariableSet,
        fetch, offset);

    call.transformTo(newEnrichedJoin);
  }
}
