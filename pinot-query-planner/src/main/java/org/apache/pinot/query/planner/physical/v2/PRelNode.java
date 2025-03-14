package org.apache.pinot.query.planner.physical.v2;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.pinot.calcite.rel.PinotDataDistribution;
import org.apache.pinot.calcite.rel.logical.PinotLogicalAggregate;


/**
 * Wrapper around Calcite RelNodes to allow tracking metadata without having to deal with RelMetadataQuery and the
 * like. The tree formed by PRelNode and RelNode should always be the same.
 */
public class PRelNode {
  private final int _nodeId;
  private final RelNode _relNode;
  @Nullable
  private final PinotDataDistribution _pinotDataDistribution;
  private final List<PRelNode> _inputs;
  private final boolean _leafStage;
  @Nullable
  private final TableScanMetadata _tableScanMetadata;

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution) {
    this(nodeId, relNode, pinotDataDistribution, Collections.emptyList());
  }

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution,
      List<PRelNode> inputs) {
    this(nodeId, relNode, pinotDataDistribution, inputs, false, null);
  }

  public PRelNode(int nodeId, RelNode relNode, @Nullable PinotDataDistribution pinotDataDistribution,
      List<PRelNode> inputs, boolean leafStage, TableScanMetadata tableScanMetadata) {
    _nodeId = nodeId;
    _relNode = relNode;
    _pinotDataDistribution = pinotDataDistribution;
    _inputs = Collections.unmodifiableList(inputs);
    _leafStage = leafStage;
    _tableScanMetadata = tableScanMetadata;
  }

  public PRelNode withPinotDataDistribution(PinotDataDistribution newDistribution) {
    Preconditions.checkNotNull(newDistribution, "Attempted to set null distribution in PRelNode");
    return new PRelNode(_nodeId, _relNode, newDistribution, _inputs, _leafStage, _tableScanMetadata);
  }

  public PRelNode with(PinotDataDistribution newDistribution, TableScanMetadata tableScanMetadata) {
    Preconditions.checkNotNull(newDistribution, "Attempted to set null distribution in PRelNode");
    return new PRelNode(_nodeId, _relNode, newDistribution, _inputs, _leafStage, _tableScanMetadata);
  }

  public PRelNode withNewInputs(int nodeId, List<PRelNode> newPRelInputs, PinotDataDistribution pinotDataDistribution) {
    List<RelNode> newRelInputs = new ArrayList<>();
    for (PRelNode newPRelInput : newPRelInputs) {
      newRelInputs.add(newPRelInput.getRelNode());
    }
    RelNode relNode = _relNode.copy(_relNode.getTraitSet(), newRelInputs);
    return new PRelNode(nodeId, relNode, pinotDataDistribution, newPRelInputs, _leafStage, _tableScanMetadata);
  }

  /**
   * Converts this node to a leaf stage.
   */
  public PRelNode asLeafStage(Supplier<Integer> idGenerator) {
    return new PRelNode(idGenerator.get(), _relNode, _pinotDataDistribution, _inputs, true, _tableScanMetadata);
  }

  public int getNodeId() {
    return _nodeId;
  }

  public RelNode getRelNode() {
    return _relNode;
  }

  public boolean hasPinotDataDistribution() {
    return _pinotDataDistribution != null;
  }

  @Nullable
  public PinotDataDistribution getPinotDataDistribution() {
    return _pinotDataDistribution;
  }

  public PinotDataDistribution getPinotDataDistributionOrThrow() {
    Preconditions.checkNotNull(_pinotDataDistribution, "No data distribution assigned to node");
    return _pinotDataDistribution;
  }

  public boolean isLeafStage() {
    return _leafStage;
  }

  public List<PRelNode> getInputs() {
    return _inputs;
  }

  public PRelNode getInput(int index) {
    return _inputs.get(index);
  }

  @Nullable
  public TableScanMetadata getTableScanMetadata() {
    return _tableScanMetadata;
  }

  public static PRelNode wrapRelTree(RelNode relNode, Supplier<Integer> nodeIdSupplier) {
    List<PRelNode> newInputs = new ArrayList<>();
    for (RelNode input : relNode.getInputs()) {
      newInputs.add(wrapRelTree(input, nodeIdSupplier));
    }
    return new PRelNode(nodeIdSupplier.get(), relNode, null, newInputs);
  }

  public static void printWrappedRelNode(PRelNode currentNode, int level) {
    if (level > 0) {
      System.err.print("|");
    }
    for (int i = 0; i < level * 4; i++) {
      System.err.print("-");
    }
    System.err.printf("%s (nodeId=%d) %n", printRelDetails(currentNode.getRelNode()), currentNode.getNodeId());
    for (PRelNode input : currentNode.getInputs()) {
      printWrappedRelNode(input, level + 1);
    }
  }

  private static String printRelDetails(RelNode relNode) {
    if (relNode instanceof PinotLogicalAggregate) {
      PinotLogicalAggregate aggregate = (PinotLogicalAggregate) relNode;
      return String.format("%s (aggType=%s, limit=%s, collations=%s, leafReturnFinalResult=%s)",
          aggregate.getRelTypeName(), aggregate.getAggType(), aggregate.getLimit(), aggregate.getCollations(),
          aggregate.isLeafReturnFinalResult());
    }
    return relNode.getRelTypeName();
  }
}
