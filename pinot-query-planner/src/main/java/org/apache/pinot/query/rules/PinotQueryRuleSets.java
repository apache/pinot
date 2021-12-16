package org.apache.pinot.query.rules;

import java.util.Arrays;
import java.util.Collection;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateUnionAggregateRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectSortTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.UnionEliminatorRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;


/**
 * Default rule sets for Pinot query
 */
public class PinotQueryRuleSets {
  public static final Collection<RelOptRule> LOGICAL_OPT_RULES = Arrays.asList(
      EnumerableRules.ENUMERABLE_FILTER_RULE,
      EnumerableRules.ENUMERABLE_JOIN_RULE,
      EnumerableRules.ENUMERABLE_PROJECT_RULE,
      EnumerableRules.ENUMERABLE_SORT_RULE,
      EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,

      // push a filter into a join
      FilterJoinRule.FILTER_ON_JOIN,
      // push filter into the children of a join
      FilterJoinRule.JOIN,
      // push filter through an aggregation
      FilterAggregateTransposeRule.INSTANCE,
      // push filter through set operation
      FilterSetOpTransposeRule.INSTANCE,
      // push project through set operation
      ProjectSetOpTransposeRule.INSTANCE,

      // aggregation and projection rules
      AggregateProjectMergeRule.INSTANCE,
      AggregateProjectPullUpConstantsRule.INSTANCE,
      // push a projection past a filter or vice versa
      ProjectFilterTransposeRule.INSTANCE,
      FilterProjectTransposeRule.INSTANCE,
      // push a projection to the children of a join
      // push all expressions to handle the time indicator correctly
      new ProjectJoinTransposeRule(PushProjector.ExprCondition.FALSE, RelFactories.LOGICAL_BUILDER),
      // merge projections
      ProjectMergeRule.INSTANCE,
      // remove identity project
      ProjectRemoveRule.INSTANCE,
      // reorder sort and projection
      SortProjectTransposeRule.INSTANCE,
      ProjectSortTransposeRule.INSTANCE,

      // join rules
      JoinPushExpressionsRule.INSTANCE,

      // remove union with only a single child
      UnionEliminatorRule.INSTANCE,
      // convert non-all union into all-union + distinct
      UnionToDistinctRule.INSTANCE,

      // remove aggregation if it does not aggregate and input is already distinct
      AggregateRemoveRule.INSTANCE,
      // push aggregate through join
      AggregateJoinTransposeRule.EXTENDED,
      // aggregate union rule
      AggregateUnionAggregateRule.INSTANCE,

      // reduce aggregate functions like AVG, STDDEV_POP etc.
      AggregateReduceFunctionsRule.INSTANCE,

      // remove unnecessary sort rule
      SortRemoveRule.INSTANCE,

      // calc rules
      FilterCalcMergeRule.INSTANCE, ProjectCalcMergeRule.INSTANCE, FilterToCalcRule.INSTANCE,
      ProjectToCalcRule.INSTANCE, CalcMergeRule.INSTANCE,

      // prune empty results rules
      PruneEmptyRules.AGGREGATE_INSTANCE, PruneEmptyRules.FILTER_INSTANCE, PruneEmptyRules.JOIN_LEFT_INSTANCE,
      PruneEmptyRules.JOIN_RIGHT_INSTANCE, PruneEmptyRules.PROJECT_INSTANCE, PruneEmptyRules.SORT_INSTANCE,
      PruneEmptyRules.UNION_INSTANCE,

      // Pinot specific rules
      PinotExchangeNodeInsertRule.INSTANCE
      );
}
