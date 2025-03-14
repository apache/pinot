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
package org.apache.pinot.query.planner.physical;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.TableScan;
import org.apache.commons.collections4.MapUtils;
import org.apache.pinot.calcite.rel.ExchangeStrategy;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotPhysicalExchange;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.v2.PRelNode;
import org.apache.pinot.query.planner.physical.v2.PRelToPlanNodeConverter;
import org.apache.pinot.query.planner.physical.v2.TableScanMetadata;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.SharedMailboxInfos;


public class PlanFragmentAndMailboxAssignment {
  private static final int ROOT_FRAGMENT_ID = 0;
  private static final int FIRST_NON_ROOT_FRAGMENT_ID = 1;

  public Result compute(PRelNode rootPRelNode, PhysicalPlannerContext physicalPlannerContext) {
    Preconditions.checkState(!(rootPRelNode.getRelNode() instanceof Exchange), "root node should never be exchange");
    final DataSchema rootDataSchema = PRelToPlanNodeConverter.toDataSchema(rootPRelNode.getRelNode().getRowType());
    // Create input fragment's send node.
    MailboxSendNode sendNode = new MailboxSendNode(FIRST_NON_ROOT_FRAGMENT_ID, rootDataSchema, new ArrayList<>(),
        ROOT_FRAGMENT_ID, PinotRelExchangeType.getDefaultExchangeType(), RelDistribution.Type.SINGLETON,
        null, false, null, false);
    // Create root receive node.
    MailboxReceiveNode rootReceiveNode = new MailboxReceiveNode(ROOT_FRAGMENT_ID, rootDataSchema,
        FIRST_NON_ROOT_FRAGMENT_ID, PinotRelExchangeType.getDefaultExchangeType(),
        RelDistribution.Type.BROADCAST_DISTRIBUTED, null, null, false, false, sendNode);
    // Create the first two fragments.
    Context context = new Context(physicalPlannerContext);
    PlanFragment rootFragment = createFragment(ROOT_FRAGMENT_ID, rootReceiveNode, new ArrayList<>(), context);
    PlanFragment firstInputFragment = createFragment(FIRST_NON_ROOT_FRAGMENT_ID, sendNode, new ArrayList<>(), context);
    rootFragment.getChildren().add(firstInputFragment);
    QueryServerInstance brokerInstance = new QueryServerInstance(physicalPlannerContext.getInstanceId(),
        physicalPlannerContext.getHostName(), physicalPlannerContext.getPort(), physicalPlannerContext.getPort());
    computeMailboxInfos(FIRST_NON_ROOT_FRAGMENT_ID, ROOT_FRAGMENT_ID,
        createWorkerMap(rootPRelNode.getPinotDataDistributionOrThrow().getWorkers(), context),
        ImmutableMap.of(0, brokerInstance), ExchangeStrategy.SINGLETON_EXCHANGE, context);
    // Traverse entire tree.
    context._fragmentMetadataMap.get(ROOT_FRAGMENT_ID).setWorkerIdToServerInstanceMap(ImmutableMap.of(
        0, brokerInstance));
    visit(rootPRelNode, sendNode, firstInputFragment, context);
    Result result = new Result();
    result._fragmentMetadataMap = context._fragmentMetadataMap;
    result._planFragmentMap = context._planFragmentMap;
    return result;
  }

  /**
   * Invariants: 1. Parent PlanNode does not have current node in input yet. 2. This node is NOT the fragment root. This
   * is because each fragment root is a MailboxSendNode.
   */
  private void visit(PRelNode pRelNode, @Nullable PlanNode parent, PlanFragment currentFragment, Context context) {
    int currentFragmentId = currentFragment.getFragmentId();
    DispatchablePlanMetadata fragmentMetadata = context._fragmentMetadataMap.get(currentFragmentId);
    if (MapUtils.isEmpty(fragmentMetadata.getWorkerIdToServerInstanceMap())) {
      // TODO: This is quite a complex invariant.
      fragmentMetadata.setWorkerIdToServerInstanceMap(createWorkerMap(
          pRelNode.getPinotDataDistributionOrThrow().getWorkers(), context));
    }
    if (pRelNode.getRelNode() instanceof TableScan) {
      TableScanMetadata tableScanMetadata = Objects.requireNonNull(pRelNode.getTableScanMetadata(),
          "No metadata in table scan PRelNode");
      String tableName = tableScanMetadata.getScannedTables().stream().findFirst().orElseThrow();
      if (!tableScanMetadata.getUnavailableSegmentsMap().isEmpty()) {
        fragmentMetadata.addUnavailableSegments(tableName,
            tableScanMetadata.getUnavailableSegmentsMap().get(tableName));
      }
      fragmentMetadata.addScannedTable(tableName);
      fragmentMetadata.setWorkerIdToSegmentsMap(tableScanMetadata.getWorkedIdToSegmentsMap());
      NodeHint nodeHint = NodeHint.fromRelHints(((TableScan) pRelNode.getRelNode()).getHints());
      fragmentMetadata.setTableOptions(nodeHint.getHintOptions().get(PinotHintOptions.TABLE_HINT_OPTIONS));
      if (tableScanMetadata.getTimeBoundaryInfo() != null) {
        fragmentMetadata.setTimeBoundaryInfo(tableScanMetadata.getTimeBoundaryInfo());
      }
    }
    if (pRelNode.getRelNode() instanceof PinotPhysicalExchange) {
      PinotPhysicalExchange physicalExchange = (PinotPhysicalExchange) pRelNode.getRelNode();
      int senderFragmentId = context._planFragmentMap.size();
      final DataSchema inputFragmentSchema = PRelToPlanNodeConverter.toDataSchema(
          pRelNode.getInput(0).getRelNode().getRowType());
      RelDistribution.Type distributionType = inferDistributionType(physicalExchange.getExchangeStrategy());
      List<PlanNode> inputs = new ArrayList<>();
      MailboxSendNode sendNode = new MailboxSendNode(senderFragmentId, inputFragmentSchema, inputs, currentFragmentId,
          PinotRelExchangeType.getDefaultExchangeType(), distributionType, physicalExchange.getKeys(), false,
          physicalExchange.getCollation().getFieldCollations(), false /* todo: set sortOnSender */);
      MailboxReceiveNode receiveNode = new MailboxReceiveNode(currentFragmentId, inputFragmentSchema,
          senderFragmentId, PinotRelExchangeType.getDefaultExchangeType(), distributionType, physicalExchange.getKeys(),
          physicalExchange.getCollation().getFieldCollations(), false /* TODO: set sort on receiver */,
          false /* TODO: set sort on sender */, sendNode);
      PlanFragment newPlanFragment = createFragment(senderFragmentId, sendNode, new ArrayList<>(), context);
      Map<Integer, QueryServerInstance> senderWorkers = createWorkerMap(pRelNode.getInput(0)
          .getPinotDataDistributionOrThrow().getWorkers(), context);
      Map<Integer, QueryServerInstance> receiverWorkers = createWorkerMap(pRelNode.getPinotDataDistributionOrThrow()
          .getWorkers(), context);
      computeMailboxInfos(senderFragmentId, currentFragmentId, senderWorkers, receiverWorkers,
          physicalExchange.getExchangeStrategy(), context);
      currentFragment.getChildren().add(newPlanFragment);
      visit(pRelNode.getInput(0), sendNode, newPlanFragment, context);
      if (parent != null) {
        parent.getInputs().add(receiveNode);
      }
      return;
    }
    PlanNode planNode = PRelToPlanNodeConverter.toPlanNode(pRelNode, currentFragmentId);
    for (PRelNode input : pRelNode.getInputs()) {
      visit(input, planNode, currentFragment, context);
    }
    if (parent != null) {
      parent.getInputs().add(planNode);
    }
  }

  private PlanFragment createFragment(int fragmentId, PlanNode planNode, List<PlanFragment> inputFragments,
      Context context) {
    PlanFragment fragment = new PlanFragment(fragmentId, planNode, inputFragments);
    context._planFragmentMap.put(fragmentId, fragment);
    context._fragmentMetadataMap.put(fragmentId, new DispatchablePlanMetadata());
    return fragment;
  }

  private RelDistribution.Type inferDistributionType(ExchangeStrategy desc) {
    RelDistribution.Type distributionType;
    switch (desc) {
      case PARTITIONING_EXCHANGE:
        distributionType = RelDistribution.Type.HASH_DISTRIBUTED;
        break;
      case IDENTITY_EXCHANGE:
        distributionType = RelDistribution.Type.HASH_DISTRIBUTED;
        break;
      case SINGLETON_EXCHANGE:
        distributionType = RelDistribution.Type.SINGLETON;
        break;
      case BROADCAST_EXCHANGE:
        distributionType = RelDistribution.Type.BROADCAST_DISTRIBUTED;
        break;
      default:
        throw new IllegalStateException("");
    }
    return distributionType;
  }

  private Map<Integer, QueryServerInstance> createWorkerMap(List<String> workers, Context context) {
    Map<Integer, QueryServerInstance> mp = new HashMap<>();
    for (String instance : workers) {
      String[] splits = instance.split("@");
      mp.put(Integer.parseInt(splits[0]),
          context._physicalPlannerContext.getInstanceIdToQueryServerInstance().get(splits[1]));
    }
    return mp;
  }

  private void computeMailboxInfos(int senderStageId, int receiverStageId,
      Map<Integer, QueryServerInstance> senderWorkers, Map<Integer, QueryServerInstance> receiverWorkers,
      ExchangeStrategy exchangeDesc, Context context) {
    DispatchablePlanMetadata senderMetadata = context._fragmentMetadataMap.get(senderStageId);
    DispatchablePlanMetadata receiverMetadata = context._fragmentMetadataMap.get(receiverStageId);
    Map<Integer, Map<Integer, MailboxInfos>> senderMailboxMap = senderMetadata.getWorkerIdToMailboxesMap();
    Map<Integer, Map<Integer, MailboxInfos>> receiverMailboxMap = receiverMetadata.getWorkerIdToMailboxesMap();
    switch (exchangeDesc) {
      case IDENTITY_EXCHANGE:
        Preconditions.checkState(senderWorkers.size() == receiverWorkers.size(),
            "Identity exchange should mean same number of workers in sender/receiver");
        for (int workerId = 0; workerId < senderWorkers.size(); workerId++) {
          QueryServerInstance senderWorker = senderWorkers.get(workerId);
          QueryServerInstance receiverWorker = receiverWorkers.get(workerId);
          MailboxInfos mailboxInfosForSender = new SharedMailboxInfos(new MailboxInfo(receiverWorker.getHostname(),
              receiverWorker.getQueryMailboxPort(), ImmutableList.of(workerId)));
          MailboxInfos mailboxInfosForReceiver = new SharedMailboxInfos(new MailboxInfo(senderWorker.getHostname(),
              senderWorker.getQueryMailboxPort(), ImmutableList.of(workerId)));
          senderMailboxMap.computeIfAbsent(workerId, (x) -> new HashMap<>()).put(receiverStageId,
              mailboxInfosForSender);
          receiverMailboxMap.computeIfAbsent(workerId, (x) -> new HashMap<>()).put(
              senderStageId, mailboxInfosForReceiver);
        }
        break;
      case SINGLETON_EXCHANGE: {
        Preconditions.checkState(receiverWorkers.size() == 1, "Singleton expects single instance in receiver");
        QueryServerInstance receiverWorker = receiverWorkers.get(0);
        MailboxInfos mailboxInfosForSender = new SharedMailboxInfos(
            new MailboxInfo(receiverWorker.getHostname(), receiverWorker.getQueryMailboxPort(), ImmutableList.of(0)));
        for (int workerId = 0; workerId < senderWorkers.size(); workerId++) {
          senderMailboxMap.computeIfAbsent(workerId, (x) -> new HashMap<>())
              .put(receiverStageId, mailboxInfosForSender);
        }
        List<MailboxInfo> mailboxInfoListForReceiver = createMailboxInfo(senderWorkers);
        receiverMailboxMap.computeIfAbsent(0, (x) -> new HashMap<>())
            .put(senderStageId, new SharedMailboxInfos(mailboxInfoListForReceiver));
        break;
      }
      case PARTITIONING_EXCHANGE:
      case BROADCAST_EXCHANGE: {
        MailboxInfos mailboxInfoListForSender = new MailboxInfos(createMailboxInfo(receiverWorkers));
        MailboxInfos mailboxInfoListForReceiver = new MailboxInfos(createMailboxInfo(senderWorkers));
        for (int receiverWorkerId = 0; receiverWorkerId < receiverWorkers.size(); receiverWorkerId++) {
          receiverMailboxMap.computeIfAbsent(receiverWorkerId, (x) -> new HashMap<>()).put(senderStageId,
              mailboxInfoListForReceiver);
        }
        for (int senderWorkerId = 0; senderWorkerId < senderWorkers.size(); senderWorkerId++) {
          senderMailboxMap.computeIfAbsent(senderWorkerId, (x) -> new HashMap<>()).put(receiverStageId,
              mailboxInfoListForSender);
        }
        break;
      }
      default:
        throw new UnsupportedOperationException("exchange desc not supported yet: " + exchangeDesc);
    }
  }

  private static List<MailboxInfo> createMailboxInfo(Map<Integer, QueryServerInstance> workers) {
    Map<QueryServerInstance, List<Integer>> workersByUniqueHostPort = new HashMap<>();
    for (var entry : workers.entrySet()) {
      workersByUniqueHostPort.computeIfAbsent(entry.getValue(), (x) -> new ArrayList<>()).add(entry.getKey());
    }
    List<MailboxInfo> result = new ArrayList<>();
    for (var entry : workersByUniqueHostPort.entrySet()) {
      result.add(new MailboxInfo(entry.getKey().getHostname(), entry.getKey().getQueryMailboxPort(), entry.getValue()));
    }
    return result;
  }

  public static class Context {
    final PhysicalPlannerContext _physicalPlannerContext;
    Map<Integer, PlanFragment> _planFragmentMap = new HashMap<>();
    Map<Integer, DispatchablePlanMetadata> _fragmentMetadataMap = new HashMap<>();

    public Context(PhysicalPlannerContext physicalPlannerContext) {
      _physicalPlannerContext = physicalPlannerContext;
    }
  }

  public static class Result {
    public Map<Integer, PlanFragment> _planFragmentMap;
    public Map<Integer, DispatchablePlanMetadata> _fragmentMetadataMap;
  }
}
