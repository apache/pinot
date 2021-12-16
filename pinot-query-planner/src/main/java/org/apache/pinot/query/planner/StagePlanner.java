package org.apache.pinot.query.planner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.pinot.query.context.PlannerContext;
import org.apache.pinot.query.planner.nodes.MailboxReceiveNode;
import org.apache.pinot.query.planner.nodes.MailboxSendNode;
import org.apache.pinot.query.planner.nodes.StageNode;


/**
 * QueryPlanMaker walks top-down from {@link RelRoot} and construct a forest of trees with {@link StageNode}.
 *
 * This class is non-threadsafe. Do not reuse the stage planner for multiple query plans.
 */
public class StagePlanner {
  private final PlannerContext _PlannerContext;

  public StagePlanner(PlannerContext PlannerContext) {
    _PlannerContext = PlannerContext;
  }

  public Map<String, StageNode>  makePlan(RelNode relRoot) {
    Map<String, StageNode> queryStageRootMap = new HashMap<>();
    StageNode globalStageRoot = walkRelPlan(queryStageRootMap, relRoot, getNewStageId());

    // global root needs to send results back to the ROOT, a.k.a. the client response node.
    StageNode globalReceiverNode = new MailboxReceiveNode("ROOT", globalStageRoot.getStageId());
    StageNode globalSenderNode = new MailboxSendNode(globalStageRoot, globalReceiverNode.getStageId());
    queryStageRootMap.put(globalSenderNode.getStageId(), globalSenderNode);
    return queryStageRootMap;
  }

  // non-threadsafe
  private StageNode walkRelPlan(final Map<String, StageNode> queryStageRootMap, RelNode node, String currentStageId) {
    if (isExchangeNode(node)) {
      // 1. exchangeNode always have only one input, get its input converted as a new stage root.
      StageNode nextStageRoot = walkRelPlan(queryStageRootMap, node.getInput(0), getNewStageId());
      // 2. make an exchange sender and receiver node pair
      StageNode mailboxReceiver = new MailboxReceiveNode(currentStageId, nextStageRoot.getStageId());
      StageNode mailboxSender = new MailboxSendNode(nextStageRoot, mailboxReceiver.getStageId());

      // 3. put the sender side as a completed stage.
      queryStageRootMap.put(mailboxSender.getStageId(), mailboxSender);

      // 4. return the receiver (this is considered as a "virtual table scan" node for its parent.
      return mailboxReceiver;
    } else {
      StageNode stageNode = StageNodeConverter.toStageNode(node, currentStageId);
      List<RelNode> inputs = node.getInputs();
      for (RelNode input : inputs) {
        stageNode.addInput(walkRelPlan(queryStageRootMap, input, currentStageId));
      }
      return stageNode;
    }
  }

  private boolean isExchangeNode(RelNode node) {
    return (node instanceof LogicalExchange);
  }

  private static String getNewStageId() {
    return UUID.randomUUID().toString();
  }
}
