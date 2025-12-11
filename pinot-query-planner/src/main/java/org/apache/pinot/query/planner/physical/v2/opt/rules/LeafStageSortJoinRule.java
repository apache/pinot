package org.apache.pinot.query.planner.physical.v2.opt.rules;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalJoin;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalSort;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * If the user has attempted to leverage a Sorted join on top of leaf stage inputs, then this rule
 * attempts to add a Sort to the leaf stage to avoid an unnecessary exchange. This only applies when the
 * following conditions are met:
 * <ul>
 *     <li>User has explicitly requested a sorted join strategy.</li>
 *     <li>Both inputs to the join are leaf stage nodes.</li>
 *     <li>Join is an equi join with only 1 key. This key becomes the sort key for the join.</li>
 *     <li>Both inputs are already partitioned and satisfy their dist constraints.</li>
 *     <li>Root of leaf stage is either of table-scan, project or filter.</li>
 * </ul>
 */
public class LeafStageSortJoinRule extends PRelOptRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeafStageSortJoinRule.class);
  private final PhysicalPlannerContext _physicalPlannerContext;

  public LeafStageSortJoinRule(PhysicalPlannerContext physicalPlannerContext) {
    _physicalPlannerContext = physicalPlannerContext;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    if (call._currentNode instanceof PhysicalJoin) {
      PhysicalJoin join = (PhysicalJoin) call._currentNode;
      if (PinotHintOptions.JoinHintOptions.useSortedJoinStrategy(join)) {
        return !join.isLeafStage() && join.getPRelInput(0).isLeafStage() && join.getPRelInput(1).isLeafStage();
      }
    }
    return false;
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PhysicalJoin join = (PhysicalJoin) call._currentNode;
    Pair<RelCollation, RelCollation> sortConstraints = getSortConstraints(join);
    if (sortConstraints == null) {
      return join;
    }
    PRelNode newLeft = addSortToLeafStage(join.getPRelInput(0), sortConstraints.getLeft());
    PRelNode newRight = addSortToLeafStage(join.getPRelInput(1), sortConstraints.getRight());
    if (newLeft == null || newRight == null) {
      return join;
    }
    return join.with(List.of(newLeft, newRight));
  }

  @Nullable
  private PRelNode addSortToLeafStage(PRelNode node, RelCollation collation) {
    if (!node.isLeafStage() || !satisfiesDistTrait(node) || node.unwrap().getTraitSet().getCollation() == null) {
      return null;
    }
    if (node.unwrap() instanceof TableScan || node.unwrap() instanceof Filter || node.unwrap() instanceof Project) {
      PinotDataDistribution pdd = node.getPinotDataDistributionOrThrow().withCollation(collation);
      return new PhysicalSort(node.unwrap().getCluster(), RelTraitSet.createEmpty(), List.of(), collation, null, null,
          node, _physicalPlannerContext.getNodeIdGenerator().get(), pdd, true);
    }
    return null;
  }

  private boolean satisfiesDistTrait(PRelNode node) {
    PinotDataDistribution pdd = node.getPinotDataDistributionOrThrow();
    RelDistribution distConstraint = node.unwrap().getTraitSet().getDistribution();
    return pdd.satisfies(distConstraint);
  }

  @Nullable
  private Pair<RelCollation, RelCollation> getSortConstraints(PhysicalJoin join) {
    JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.isEqui() || joinInfo.leftKeys.size() != 1) {
      return null;
    }
    int leftKey = joinInfo.leftKeys.get(0);
    int rightKey = joinInfo.rightKeys.get(0);
    RelCollation leftCollation = RelCollations.of(leftKey);
    RelCollation rightCollation = RelCollations.of(rightKey);
    return Pair.of(leftCollation, rightCollation);
  }
}
