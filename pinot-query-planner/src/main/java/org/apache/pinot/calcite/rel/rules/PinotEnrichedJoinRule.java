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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotLogicalEnrichedJoin;
import org.apache.pinot.spi.utils.CommonConstants;
import org.immutables.value.Value;


/**
 * Rules for fusing filter, projection, and limit operators into join and enriched joins.
 * This collection of rules will be fired bottom-up and fuse operators into the enriched join greedily.
 */
@Value.Enclosing
public class PinotEnrichedJoinRule {

  private PinotEnrichedJoinRule() {
  }

  /**
   * Rule that fuses a LogicalProject into a PinotLogicalEnrichedJoin
   */
  public static class ProjectEnrichedJoin extends RelRule<ProjectEnrichedJoin.ProjectEnrichedJoinConfig> {

    @Value.Immutable
    public interface ProjectEnrichedJoinConfig extends RelRule.Config {
      ProjectEnrichedJoinConfig DEFAULT =
          ImmutablePinotEnrichedJoinRule.ProjectEnrichedJoinConfig.builder()
              .operandSupplier(b0 ->
                  b0.operand(LogicalProject.class).oneInput(b1 ->
                      b1.operand(PinotLogicalEnrichedJoin.class).anyInputs()
                  )
              ).build();

      @Override
      default ProjectEnrichedJoin toRule() {
        return new ProjectEnrichedJoin(this);
      }
    }

    private ProjectEnrichedJoin(ProjectEnrichedJoinConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      PinotLogicalEnrichedJoin enrichedJoin = call.rel(1);

      // Add projection to the enriched join
      PinotLogicalEnrichedJoin newEnrichedJoin = enrichedJoin.withNewProject(
          new PinotLogicalEnrichedJoin.FilterProjectRexNode(project.getProjects(), project.getRowType()),
          project.getRowType(),
          project.getVariablesSet());

      call.transformTo(newEnrichedJoin);
    }
  }

  /**
   * Rule that fuses a LogicalFilter into a PinotLogicalEnrichedJoin
   */
  public static class FilterEnrichedJoin extends RelRule<FilterEnrichedJoin.FilterEnrichedJoinConfig> {

    @Value.Immutable
    public interface FilterEnrichedJoinConfig extends RelRule.Config {
      FilterEnrichedJoinConfig DEFAULT =
          ImmutablePinotEnrichedJoinRule.FilterEnrichedJoinConfig.builder()
              .operandSupplier(b0 ->
                  b0.operand(LogicalFilter.class).oneInput(b1 ->
                      b1.operand(PinotLogicalEnrichedJoin.class).anyInputs()
                  )
              ).build();

      @Override
      default FilterEnrichedJoin toRule() {
        return new FilterEnrichedJoin(this);
      }
    }

    private FilterEnrichedJoin(FilterEnrichedJoinConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      PinotLogicalEnrichedJoin enrichedJoin = call.rel(1);

      // Add filter to the enriched join
      PinotLogicalEnrichedJoin newEnrichedJoin = enrichedJoin.withNewFilter(
          new PinotLogicalEnrichedJoin.FilterProjectRexNode(filter.getCondition()));

      call.transformTo(newEnrichedJoin);
    }
  }

  /**
   * Rule that fuses a LogicalSort into a PinotLogicalEnrichedJoin
   */
  public static class SortEnrichedJoin extends RelRule<SortEnrichedJoin.SortEnrichedJoinConfig> {

    @Value.Immutable
    public interface SortEnrichedJoinConfig extends RelRule.Config {
      SortEnrichedJoinConfig DEFAULT = ImmutablePinotEnrichedJoinRule.SortEnrichedJoinConfig.builder()
          .operandSupplier(b0 ->
              b0.operand(LogicalSort.class).oneInput(b1 ->
                  b1.operand(PinotLogicalEnrichedJoin.class).anyInputs()
              )
          ).build();

      @Override
      default SortEnrichedJoin toRule() {
        return new SortEnrichedJoin(this);
      }
    }

    private SortEnrichedJoin(SortEnrichedJoinConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalSort sort = call.rel(0);
      PinotLogicalEnrichedJoin enrichedJoin = call.rel(1);

      // If enriched join already had a sort operator merged, return
      if (enrichedJoin.getOffset() != null || enrichedJoin.getFetch() != null) {
        return;
      }

      // Enriched join does not support sort collation, only fetch and offset
      if (sort.getCollation() != null && !sort.getCollation().equals(RelCollations.EMPTY)) {
        return;
      }

      // Add sort limit to the enriched join
      PinotLogicalEnrichedJoin newEnrichedJoin = enrichedJoin.withNewFetchOffset(sort.fetch, sort.offset);
      call.transformTo(newEnrichedJoin);
    }
  }

  /**
   * Rule that converts LogicalProject + LogicalJoin into PinotLogicalEnrichedJoin
   */
  public static class ProjectJoin extends RelRule<ProjectJoin.ProjectJoinConfig> {

    @Value.Immutable
    public interface ProjectJoinConfig extends RelRule.Config {
      ProjectJoinConfig DEFAULT = ImmutablePinotEnrichedJoinRule.ProjectJoinConfig.builder()
          .operandSupplier(b0 ->
              b0.operand(LogicalProject.class).oneInput(b1 ->
                  b1.operand(LogicalJoin.class).anyInputs()
              )
          ).build();

      @Override
      default ProjectJoin toRule() {
        return new ProjectJoin(this);
      }
    }

    private ProjectJoin(ProjectJoinConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalProject project = call.rel(0);
      LogicalJoin join = call.rel(1);

      if (!canConvertJoin(join)) {
        return;
      }

      List<PinotLogicalEnrichedJoin.FilterProjectRexNode> filterProjectRexNodes = new ArrayList<>();
      filterProjectRexNodes.add(
          new PinotLogicalEnrichedJoin.FilterProjectRexNode(project.getProjects(), project.getRowType()));

      PinotLogicalEnrichedJoin enrichedJoin = new PinotLogicalEnrichedJoin(
          join.getCluster(),
          join.getTraitSet(),
          join.getHints(),
          join.getInput(0),
          join.getInput(1),
          join.getCondition(),
          join.getVariablesSet(),
          join.getJoinType(),
          filterProjectRexNodes,
          project.getRowType(),
          project.getVariablesSet(),
          null,
          null);

      call.transformTo(enrichedJoin);
    }
  }

  /**
   * Rule that converts LogicalFilter + LogicalJoin into PinotLogicalEnrichedJoin
   */
  public static class FilterJoin extends RelRule<FilterJoin.FilterJoinConfig> {

    @Value.Immutable
    public interface FilterJoinConfig extends RelRule.Config {
      FilterJoinConfig DEFAULT = ImmutablePinotEnrichedJoinRule.FilterJoinConfig.builder()
          .operandSupplier(b0 ->
              b0.operand(LogicalFilter.class).oneInput(b1 ->
                  b1.operand(LogicalJoin.class).anyInputs()
              )
          ).build();

      @Override
      default FilterJoin toRule() {
        return new FilterJoin(this);
      }
    }

    private FilterJoin(FilterJoinConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      LogicalJoin join = call.rel(1);

      if (!canConvertJoin(join)) {
        return;
      }

      List<PinotLogicalEnrichedJoin.FilterProjectRexNode> filterProjectRexNodes = new ArrayList<>();
      filterProjectRexNodes.add(new PinotLogicalEnrichedJoin.FilterProjectRexNode(filter.getCondition()));

      PinotLogicalEnrichedJoin enrichedJoin = new PinotLogicalEnrichedJoin(
          join.getCluster(),
          join.getTraitSet(),
          join.getHints(),
          join.getInput(0),
          join.getInput(1),
          join.getCondition(),
          join.getVariablesSet(),
          join.getJoinType(),
          filterProjectRexNodes,
          null,  // No projection, final output row type is same as join output
          null,  // No projection variable set
          null,
          null);

      call.transformTo(enrichedJoin);
    }
  }

  /**
   * Rule that converts LogicalSort + LogicalJoin into PinotLogicalEnrichedJoin
   */
  public static class SortJoin extends RelRule<SortJoin.SortJoinConfig> {

    @Value.Immutable
    public interface SortJoinConfig extends RelRule.Config {
      SortJoinConfig DEFAULT = ImmutablePinotEnrichedJoinRule.SortJoinConfig.builder()
          .operandSupplier(b0 ->
              b0.operand(LogicalSort.class).oneInput(b1 ->
                  b1.operand(LogicalJoin.class).anyInputs()
              )
          ).build();

      @Override
      default SortJoin toRule() {
        return new SortJoin(this);
      }
    }

    private SortJoin(SortJoinConfig config) {
      super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalSort sort = call.rel(0);
      LogicalJoin join = call.rel(1);

      // Enriched join does not support sort collation, only fetch and offset
      if (sort.getCollation() != null && !sort.getCollation().equals(RelCollations.EMPTY)) {
        return;
      }

      if (!canConvertJoin(join)) {
        return;
      }

      List<PinotLogicalEnrichedJoin.FilterProjectRexNode> filterProjectRexNodes = new ArrayList<>();

      PinotLogicalEnrichedJoin enrichedJoin = new PinotLogicalEnrichedJoin(
          join.getCluster(),
          join.getTraitSet(),
          join.getHints(),
          join.getInput(0),
          join.getInput(1),
          join.getCondition(),
          join.getVariablesSet(),
          join.getJoinType(),
          filterProjectRexNodes,
          null,  // No projection, final output row type is same as join output
          null,  // No projection variable set
          sort.fetch,
          sort.offset);

      call.transformTo(enrichedJoin);
    }
  }

  /**
   * Temporarily disable conversion of lookup join and non-equijoin to EnrichedJoin
   */
  private static boolean canConvertJoin(LogicalJoin join) {
    // Disable lookup join for now
    if (PinotHintOptions.JoinHintOptions.useLookupJoinStrategy(join)) {
      return false;
    }

    // Disable non-equijoin for now
    if (join.analyzeCondition().leftKeys.isEmpty()) {
      return false;
    }

    return true;
  }

  // Rule instances with descriptions
  public static final ProjectEnrichedJoin PROJECT_ENRICHED_JOIN =
      (ProjectEnrichedJoin) ProjectEnrichedJoin.ProjectEnrichedJoinConfig.DEFAULT
          .withDescription(CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN)
          .toRule();

  public static final FilterEnrichedJoin FILTER_ENRICHED_JOIN =
      (FilterEnrichedJoin) FilterEnrichedJoin.FilterEnrichedJoinConfig.DEFAULT
          .withDescription(CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN)
          .toRule();

  public static final SortEnrichedJoin SORT_ENRICHED_JOIN =
      (SortEnrichedJoin) SortEnrichedJoin.SortEnrichedJoinConfig.DEFAULT
          .withDescription(CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN)
          .toRule();

  public static final ProjectJoin PROJECT_JOIN =
      (ProjectJoin) ProjectJoin.ProjectJoinConfig.DEFAULT
          .withDescription(CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN)
          .toRule();

  public static final FilterJoin FILTER_JOIN =
      (FilterJoin) FilterJoin.FilterJoinConfig.DEFAULT
          .withDescription(CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN)
          .toRule();

  public static final SortJoin SORT_JOIN =
      (SortJoin) SortJoin.SortJoinConfig.DEFAULT
          .withDescription(CommonConstants.Broker.PlannerRuleNames.JOIN_TO_ENRICHED_JOIN)
          .toRule();

  public static final List<RelOptRule> PINOT_ENRICHED_JOIN_RULES = List.of(
      PROJECT_ENRICHED_JOIN,
      FILTER_ENRICHED_JOIN,
      SORT_ENRICHED_JOIN,
      PROJECT_JOIN,
      FILTER_JOIN,
      SORT_JOIN
  );
}
