package org.apache.pinot.query.planner.physical;

import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.logical.PhysicalStageVisitor;
import org.apache.pinot.query.planner.logical.PhysicalStageVisitor.PhysicalStageInfo;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.partitioning.FieldSelectionKeySelector;
import org.apache.pinot.query.planner.partitioning.KeySelector;
import org.apache.pinot.query.planner.stage.AggregateNode;
import org.apache.pinot.query.planner.stage.FilterNode;
import org.apache.pinot.query.planner.stage.JoinNode;
import org.apache.pinot.query.planner.stage.MailboxReceiveNode;
import org.apache.pinot.query.planner.stage.MailboxSendNode;
import org.apache.pinot.query.planner.stage.ProjectNode;
import org.apache.pinot.query.planner.stage.SortNode;
import org.apache.pinot.query.planner.stage.StageNode;
import org.apache.pinot.query.planner.stage.StageNodeVisitor;
import org.apache.pinot.query.planner.stage.TableScanNode;
import org.apache.pinot.query.planner.stage.ValueNode;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Underlying assumptions:
 * 1. If a stage A depends on stage B, stageId(A) < stageId(B)
 * 2. Leaf stage can either be a MailboxReceiveNode or TableScanNode.
 */
public class SmartShuffleRewriteVisitor implements StageNodeVisitor<Set<Integer>, PhysicalStageInfo> {
  private TableCache _tableCache;
  private Map<Integer, StageMetadata> _stageMetadataMap;
  private boolean _canSkipShuffleForJoin;

  /**
   * Optimizes shuffles while maintaining partial order among high-level stages.
   */
  public static void optimizeShuffles(StageNode rootStageNode, Map<Integer, StageMetadata> stageMetadataMap,
      TableCache tableCache) {
    PhysicalStageInfo info = PhysicalStageVisitor.go(rootStageNode);
    for (int stageId = stageMetadataMap.size() - 1; stageId >= 0; stageId--) {
      StageNode stageNode = info.getRootStageNode().get(stageId);
      stageNode.visit(new SmartShuffleRewriteVisitor(tableCache, stageMetadataMap), info);
    }
  }

  private SmartShuffleRewriteVisitor(TableCache tableCache, Map<Integer, StageMetadata> stageMetadataMap) {
    _tableCache = tableCache;
    _stageMetadataMap = stageMetadataMap;
    _canSkipShuffleForJoin = false;
  }

  @Override
  public Set<Integer> visitAggregate(AggregateNode node, PhysicalStageInfo context) {
    Set<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    // any input reference directly carries over in group set of aggregation
    // should still be a partition key
    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < node.getGroupSet().size(); i++) {
      RexExpression rex = node.getGroupSet().get(i);
      if (rex instanceof RexExpression.InputRef) {
        if (oldPartitionKeys.contains(((RexExpression.InputRef) rex).getIndex())) {
          partitionKeys.add(i);
        }
      }
    }

    return partitionKeys;
  }

  @Override
  public Set<Integer> visitFilter(FilterNode node, PhysicalStageInfo context) {
    // filters don't change partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<Integer> visitJoin(JoinNode node, PhysicalStageInfo context) {
    List<MailboxReceiveNode> innerLeafNodes = context.getLeafNodes().get(node.getStageId()).stream()
        .map(x -> (MailboxReceiveNode) x).collect(Collectors.toList());
    Preconditions.checkState(innerLeafNodes.size() == 2);
    if (canServerAssignmentAllowShuffleSkip(node.getStageId(), innerLeafNodes.get(0).getSenderStageId(),
        innerLeafNodes.get(1).getSenderStageId())) {
      if (canSkipShuffleForJoin(innerLeafNodes.get(0), (MailboxSendNode) innerLeafNodes.get(0).getSender(), context)) {
        if (canSkipShuffleForJoin(innerLeafNodes.get(1), (MailboxSendNode) innerLeafNodes.get(1).getSender(), context)) {
          _stageMetadataMap.get(node.getStageId()).setServerInstances(
              _stageMetadataMap.get(innerLeafNodes.get(0).getSenderStageId()).getServerInstances());
          _canSkipShuffleForJoin = true;
        }
      }
    }

    // TODO: This is copy-pasted from ShuffleRewriteVisitor. Make it re-usable.
    Set<Integer> leftPKs = node.getInputs().get(0).visit(this, context);
    Set<Integer> rightPks = node.getInputs().get(1).visit(this, context);

    // Currently, JOIN criteria is guaranteed to only have one FieldSelectionKeySelector
    FieldSelectionKeySelector leftJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getLeftJoinKeySelector();
    FieldSelectionKeySelector rightJoinKey = (FieldSelectionKeySelector) node.getJoinKeys().getRightJoinKeySelector();

    int leftDataSchemaSize = node.getInputs().get(0).getDataSchema().size();
    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < leftJoinKey.getColumnIndices().size(); i++) {
      int leftIdx = leftJoinKey.getColumnIndices().get(i);
      int rightIdx = rightJoinKey.getColumnIndices().get(i);
      if (leftPKs.contains(leftIdx)) {
        partitionKeys.add(leftIdx);
      }
      if (rightPks.contains(rightIdx)) {
        // combined schema will have all the left fields before the right fields
        // so add the leftDataSchemaSize before adding the key
        partitionKeys.add(leftDataSchemaSize + rightIdx);
      }
    }

    return partitionKeys;
  }

  @Override
  public Set<Integer> visitMailboxReceive(MailboxReceiveNode node, PhysicalStageInfo context) {
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();
    Set<Integer> oldPartitionKeys = context.getPartitionKeys(node.getSenderStageId());
    // If the current stage is not a join-stage, then we already know sender's distribution
    if (!context.isJoinStage(node.getStageId())) {
      if (canSkipShuffle(oldPartitionKeys, selector)) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        return oldPartitionKeys;
      } else if (selector == null) {
        return new HashSet<>();
      }
      return new HashSet<>(((FieldSelectionKeySelector) selector).getColumnIndices());
    }
    // If the current stage is a join-stage then we haven't determined distribution for sender and we already know
    // whether shuffle can be skipped.
    if (_canSkipShuffleForJoin) {
      node.setExchangeType(RelDistribution.Type.SINGLETON);
      ((MailboxSendNode) node.getSender()).setExchangeType(RelDistribution.Type.SINGLETON);
      return oldPartitionKeys;
    } else if (selector == null) {
      return new HashSet<>();
    }
    return new HashSet<>(((FieldSelectionKeySelector) selector).getColumnIndices());
  }

  @Override
  public Set<Integer> visitMailboxSend(MailboxSendNode node, PhysicalStageInfo context) {
    Set<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);
    KeySelector<Object[], Object[]> selector = node.getPartitionKeySelector();

    boolean canSkipShuffleBasic = canSkipShuffle(oldPartitionKeys, selector);
    // If receiver is not a join-stage, then we can determine distribution type now.
    if (!context.isJoinStage(node.getReceiverStageId())) {
      if (canSkipShuffleBasic) {
        node.setExchangeType(RelDistribution.Type.SINGLETON);
        return oldPartitionKeys;
      }
      return new HashSet<>();
    }
    // If receiver is a join-stage, remember partition-keys of the child node of MailboxSendNode.
    Set<Integer> mailboxSendPartitionKeys = canSkipShuffleBasic ? oldPartitionKeys : new HashSet<>();
    context.setPartitionKeys(node.getStageId(), mailboxSendPartitionKeys);
    return mailboxSendPartitionKeys;
  }

  @Override
  public Set<Integer> visitProject(ProjectNode node, PhysicalStageInfo context) {
    Set<Integer> oldPartitionKeys = node.getInputs().get(0).visit(this, context);

    // all inputs carry over if they're still in the projection result
    Set<Integer> partitionKeys = new HashSet<>();
    for (int i = 0; i < node.getProjects().size(); i++) {
      RexExpression rex = node.getProjects().get(i);
      if (rex instanceof RexExpression.InputRef) {
        if (oldPartitionKeys.contains(((RexExpression.InputRef) rex).getIndex())) {
          partitionKeys.add(i);
        }
      }
    }

    return partitionKeys;
  }

  @Override
  public Set<Integer> visitSort(SortNode node, PhysicalStageInfo context) {
    // sort doesn't change the partition keys
    return node.getInputs().get(0).visit(this, context);
  }

  @Override
  public Set<Integer> visitTableScan(TableScanNode node, PhysicalStageInfo context) {
    TableConfig tableConfig =
        _tableCache.getTableConfig(node.getTableName());
    Preconditions.checkNotNull(tableConfig, "table config is null");
    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();
    if (indexingConfig != null && indexingConfig.getSegmentPartitionConfig() != null) {
      Map<String, ColumnPartitionConfig> columnPartitionMap =
          indexingConfig.getSegmentPartitionConfig().getColumnPartitionMap();
      if (columnPartitionMap != null) {
        Set<String> partitionColumns = columnPartitionMap.keySet();
        Set<Integer> newPartitionKeys = new HashSet<>();
        for (int i = 0; i < node.getTableScanColumns().size(); i++) {
          if (partitionColumns.contains(node.getTableScanColumns().get(i))) {
            newPartitionKeys.add(i);
          }
        }
        return newPartitionKeys;
      }
    }
    return new HashSet<>();
  }

  @Override
  public Set<Integer> visitValue(ValueNode node, PhysicalStageInfo context) {
    return new HashSet<>();
  }

  private boolean canServerAssignmentAllowShuffleSkip(int currentStageId, int leftStageId, int rightStageId) {
    Set<ServerInstance> leftServerInstances = new HashSet<>(_stageMetadataMap.get(leftStageId).getServerInstances());
    List<ServerInstance> rightServerInstances = _stageMetadataMap.get(rightStageId).getServerInstances();
    List<ServerInstance> currentServerInstances = _stageMetadataMap.get(currentStageId).getServerInstances();
    return leftServerInstances.containsAll(rightServerInstances) &&
        leftServerInstances.size() == rightServerInstances.size() &&
        currentServerInstances.containsAll(leftServerInstances);
  }

  private boolean canSkipShuffleForJoin(MailboxReceiveNode mailboxReceiveNode, MailboxSendNode mailboxSendNode,
      PhysicalStageInfo context) {
    Set<Integer> oldPartitionKeys = context.getPartitionKeys(mailboxSendNode.getStageId());
    KeySelector<Object[], Object[]> selector = mailboxSendNode.getPartitionKeySelector();
    if (!canSkipShuffle(oldPartitionKeys, selector)) {
      return false;
    }
    // Since shuffle can be skipped, oldPartitionsKeys == senderPartitionKeys
    selector = mailboxReceiveNode.getPartitionKeySelector();
    return canSkipShuffle(oldPartitionKeys, selector);
  }

  private static boolean canSkipShuffle(Set<Integer> partitionKeys, KeySelector<Object[], Object[]> keySelector) {
    if (!partitionKeys.isEmpty() && keySelector != null) {
      Set<Integer> targetSet = new HashSet<>(((FieldSelectionKeySelector) keySelector).getColumnIndices());
      return targetSet.containsAll(partitionKeys);
    }
    return false;
  }
}
