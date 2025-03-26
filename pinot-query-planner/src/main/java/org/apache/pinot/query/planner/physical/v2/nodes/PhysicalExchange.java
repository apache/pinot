package org.apache.pinot.query.planner.physical.v2.nodes;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PinotDataDistribution;


/**
 * Exchange is a special plan node because it is often used to change the number of streams. Since we track
 * {@link PinotDataDistribution} at node level, the question is whether we should assign this so that number of streams
 * is equal to the sender or the receiver.
 * <p>
 *   We have chosen to set it based on the receiver. The idea being that after PhysicalExchange is added, the trait
 *   constraints should ideally be satisfied between the receiver and the Exchange node. This is similar to how Calcite
 *   thinks of trait enforcement via Converter Rules.
 * </p>
 */
public class PhysicalExchange extends Exchange implements PRelNode {
  private static final RelTraitSet EMPTY_TRAIT_SET = RelTraitSet.createEmpty();
  private final int _nodeId;
  private final List<PRelNode> _pRelInputs;
  /**
   * See javadocs for {@link PhysicalExchange}.
   */
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;

  public PhysicalExchange(RelOptCluster cluster, RelDistribution distribution,
      int nodeId, PRelNode input, @Nullable PinotDataDistribution pinotDataDistribution) {
    super(cluster, EMPTY_TRAIT_SET, input.unwrap(), distribution);
    _nodeId = nodeId;
    _pRelInputs = Collections.singletonList(input);
    _pinotDataDistribution = pinotDataDistribution;
  }

  @Override
  public Exchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
    Preconditions.checkState(newInput instanceof PRelNode, "Expected input of PhysicalExchange to be a PRelNode");
    Preconditions.checkState(traitSet.isEmpty(), "Expected empty trait set for PhysicalExchange");
    return new PhysicalExchange(getCluster(), newDistribution, _nodeId, (PRelNode) newInput, _pinotDataDistribution);
  }

  @Override
  public int getNodeId() {
    return _nodeId;
  }

  @Override
  public List<PRelNode> getPRelInputs() {
    return _pRelInputs;
  }

  @Override
  public RelNode unwrap() {
    return this;
  }

  @Nullable
  @Override
  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  @Override
  public boolean isLeafStage() {
    return false;
  }

  @Override
  public PRelNode copy(int newNodeId, List<PRelNode> newInputs, PinotDataDistribution newDistribution) {
    return new PhysicalExchange(getCluster(), getDistribution(), newNodeId, newInputs.get(0), newDistribution);
  }
}
