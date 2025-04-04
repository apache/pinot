package org.apache.pinot.query.planner.physical.v2.opt.rules;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.hint.PinotHintStrategyTable;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;
import org.apache.pinot.query.planner.physical.v2.mapping.DistMappingGenerator;
import org.apache.pinot.query.planner.physical.v2.mapping.PinotDistMapping;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalAggregate;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRule;
import org.apache.pinot.query.planner.physical.v2.opt.PRelOptRuleCall;


/**
 * Often it might be possible to promote an aggregate on top of a leaf stage to be part of the leaf stage. This rule
 * handles that case. This is different from aggregate pushdown because pushdown is related to taking a decision about
 * whether we should split the aggregate over an exchange into two, whereas this rule is able to avoid the Exchange
 * altogether.
 */
public class LeafStageAggregateRule extends PRelOptRule {
  private final PhysicalPlannerContext _physicalPlannerContext;

  public LeafStageAggregateRule(PhysicalPlannerContext physicalPlannerContext) {
    _physicalPlannerContext = physicalPlannerContext;
  }

  @Override
  public boolean matches(PRelOptRuleCall call) {
    if (call._currentNode.isLeafStage()) {
      return false;
    }
    PRelNode currentNode = call._currentNode;
    if (!(currentNode.unwrap() instanceof Aggregate)) {
      return false;
    }
    if (!isProjectFilterOrScan(currentNode.getPRelInput(0).unwrap())) {
      return false;
    }
    // ==> We have: "aggregate (non-leaf stage) > project|filter|table-scan (leaf-stage)"
    PhysicalAggregate aggRel = (PhysicalAggregate) currentNode.unwrap();
    PRelNode pRelInput = aggRel.getPRelInput(0);
    if (isPartitionedByHintPresent(aggRel)) {
      Preconditions.checkState(aggRel.getInput().getTraitSet().getCollation() == null,
          "Aggregate input has sort constraint, but partition-by hint is forcing to skip exchange");
      return true;
    }
    return pRelInput.areTraitsSatisfied();
  }

  @Override
  public PRelNode onMatch(PRelOptRuleCall call) {
    PhysicalAggregate currentNode = (PhysicalAggregate) call._currentNode;
    PinotDistMapping mapping = DistMappingGenerator.compute(currentNode.getPRelInput(0).unwrap(),
        currentNode.unwrap(), null);
    PinotDataDistribution derivedDistribution = currentNode.getPRelInput(0).getPinotDataDistributionOrThrow()
        .apply(mapping);
    return currentNode.with(currentNode.getPRelInputs(), derivedDistribution);
  }

  private static boolean isPartitionedByHintPresent(PhysicalAggregate aggRel) {
    Map<String, String> hintOptions =
        PinotHintStrategyTable.getHintOptions(aggRel.getHints(), PinotHintOptions.AGGREGATE_HINT_OPTIONS);
    hintOptions = hintOptions == null ? Map.of() : hintOptions;
    return Boolean.parseBoolean(hintOptions.get(PinotHintOptions.AggregateOptions.IS_PARTITIONED_BY_GROUP_BY_KEYS));
  }

  private static boolean isProjectFilterOrScan(RelNode relNode) {
    return relNode instanceof TableScan || relNode instanceof Project || relNode instanceof Filter;
  }
}
