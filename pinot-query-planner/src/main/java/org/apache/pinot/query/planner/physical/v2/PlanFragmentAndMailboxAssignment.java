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
package org.apache.pinot.query.planner.physical.v2;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.core.TableScan;
import org.apache.pinot.calcite.rel.hint.PinotHintOptions;
import org.apache.pinot.calcite.rel.logical.PinotRelExchangeType;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.context.PhysicalPlannerContext;
import org.apache.pinot.query.planner.PlanFragment;
import org.apache.pinot.query.planner.physical.DispatchablePlanMetadata;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalExchange;
import org.apache.pinot.query.planner.physical.v2.nodes.PhysicalTableScan;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.planner.plannode.MailboxSendNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.plannode.PlanNode.NodeHint;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.SharedMailboxInfos;


/**
 * <h1>Responsibilities</h1>
 * This does the following:
 * <ul>
 *   <li>Splits plan around PhysicalExchange nodes to create plan fragments.</li>
 *   <li>Converts PRelNodes to PlanNodes.</li>
 *   <li>
 *     Creates mailboxes for connecting plan fragments. This is done simply based on the workers in the send/receive
 *     plan nodes, and the exchange strategy (identity, partitioning, etc.).
 *   </li>
 *   <li>
 *     Creates metadata for each plan fragment, which includes the scanned tables, unavailable segments, etc.
 *   </li>
 * </ul>
 * <h1>Design Note</h1>
 * This class is completely un-opinionated. The old optimizer had a lot of custom logic added to mailbox assignment,
 * but this class instead doesn't do any special handling, apart from assigning mailboxes based on the exchange
 * strategy. This is an important and conscious design choice, because it ensures division of responsibilities and
 * allows optimizer rules like worker assignment to completely own their responsibilities. This is also important for
 * keeping the optimizer maximally pluggable. (e.g. you can swap out the default worker assignment rule with a
 * custom rule like the LiteMode worker assignment rule).
 */
public class PlanFragmentAndMailboxAssignment {
  private static final int ROOT_FRAGMENT_ID = 0;

  public Result compute(PRelNode rootPRelNode, PhysicalPlannerContext physicalPlannerContext) {
    // Create the first two fragments.
    Context context = new Context(physicalPlannerContext);
    // Traverse entire tree.
    process(rootPRelNode, null, ROOT_FRAGMENT_ID, context);
    Result result = new Result();
    result._fragmentMetadataMap = context._fragmentMetadataMap;
    result._planFragmentMap = context._planFragmentMap;
    return result;
  }

  private void process(PRelNode pRelNode, @Nullable PlanNode parent, int currentFragmentId, Context context) {
    if (pRelNode.unwrap() instanceof TableScan) {
      processTableScan((PhysicalTableScan) pRelNode.unwrap(), currentFragmentId, context);
    }
    if (pRelNode.unwrap() instanceof PhysicalExchange) {
      // Split an exchange into two fragments: one for the sender and one for the receiver.
      // The sender fragment will have a MailboxSendNode and receiver a MailboxReceiveNode.
      // It is possible that the receiver fragment doesn't exist yet (e.g. when PhysicalExchange is the root node).
      // In that case, we also create it here. If it exists already, we simply re-use it.
      PhysicalExchange physicalExchange = (PhysicalExchange) pRelNode.unwrap();
      PlanFragment receiverFragment = context._planFragmentMap.get(currentFragmentId);
      int senderFragmentId = context._planFragmentMap.size() + (receiverFragment == null ? 1 : 0);
      final DataSchema inputFragmentSchema = PRelToPlanNodeConverter.toDataSchema(
          pRelNode.getPRelInput(0).unwrap().getRowType());
      RelDistribution.Type distributionType = ExchangeStrategy.getRelDistribution(
          physicalExchange.getExchangeStrategy(), physicalExchange.getDistributionKeys()).getType();
      MailboxSendNode sendNode = new MailboxSendNode(senderFragmentId, inputFragmentSchema, new ArrayList<>(),
          currentFragmentId, PinotRelExchangeType.getDefaultExchangeType(), distributionType,
          physicalExchange.getDistributionKeys(), false, physicalExchange.getRelCollation().getFieldCollations(),
          false /* sort on sender */, physicalExchange.getHashFunction().name());
      MailboxReceiveNode receiveNode = new MailboxReceiveNode(currentFragmentId, inputFragmentSchema,
          senderFragmentId, PinotRelExchangeType.getDefaultExchangeType(), distributionType,
          physicalExchange.getDistributionKeys(), physicalExchange.getRelCollation().getFieldCollations(),
          !physicalExchange.getRelCollation().getFieldCollations().isEmpty(), false, sendNode);
      if (receiverFragment == null) {
        /*
         * If the root node is an exchange, then the root fragment will not exist yet. We create it here.
         */
        receiverFragment = createFragment(currentFragmentId, receiveNode, new ArrayList<>(), context,
            pRelNode.getPinotDataDistributionOrThrow().getWorkers());
      }
      PlanFragment newPlanFragment = createFragment(senderFragmentId, sendNode, new ArrayList<>(), context,
          physicalExchange.getPRelInputs().get(0).getPinotDataDistributionOrThrow().getWorkers());
      Map<Integer, QueryServerInstance> senderWorkers = createWorkerMap(pRelNode.getPRelInput(0)
          .getPinotDataDistributionOrThrow().getWorkers(), context);
      Map<Integer, QueryServerInstance> receiverWorkers = createWorkerMap(pRelNode.getPinotDataDistributionOrThrow()
          .getWorkers(), context);
      computeMailboxInfos(senderFragmentId, currentFragmentId, senderWorkers, receiverWorkers,
          physicalExchange.getExchangeStrategy(), context);
      context._planFragmentMap.get(currentFragmentId).getChildren().add(newPlanFragment);
      process(pRelNode.getPRelInput(0), sendNode, newPlanFragment.getFragmentId(), context);
      if (parent != null) {
        parent.getInputs().add(receiveNode);
      }
      return;
    }
    // Convert PRelNode to PlanNode, and create parent/input PlanNode tree.
    PlanNode planNode = PRelToPlanNodeConverter.toPlanNode(pRelNode, currentFragmentId);
    if (context._planFragmentMap.isEmpty()) {
      /*
       * If the root-node is NOT an exchange, then we create the root fragment here. If it's an exchange, it will be
       * created in the process of handling the exchange.
       */
      createFragment(ROOT_FRAGMENT_ID, planNode, new ArrayList<>(), context,
          pRelNode.getPinotDataDistributionOrThrow().getWorkers());
    }
    for (PRelNode input : pRelNode.getPRelInputs()) {
      process(input, planNode, currentFragmentId, context);
    }
    if (parent != null) {
      parent.getInputs().add(planNode);
    }
  }

  private void processTableScan(PhysicalTableScan tableScan, int currentFragmentId, Context context) {
    DispatchablePlanMetadata fragmentMetadata = context._fragmentMetadataMap.get(currentFragmentId);
    TableScanMetadata tableScanMetadata = Objects.requireNonNull(tableScan.getTableScanMetadata(),
        "No metadata in table scan PRelNode");
    String tableName = tableScanMetadata.getScannedTables().stream().findFirst().orElseThrow();
    if (!tableScanMetadata.getUnavailableSegmentsMap().isEmpty()) {
      fragmentMetadata.addUnavailableSegments(tableName,
          tableScanMetadata.getUnavailableSegmentsMap().get(tableName));
    }
    fragmentMetadata.addScannedTable(tableName);
    fragmentMetadata.setWorkerIdToSegmentsMap(tableScanMetadata.getWorkedIdToSegmentsMap());
    NodeHint nodeHint = NodeHint.fromRelHints(tableScan.getHints());
    fragmentMetadata.setTableOptions(nodeHint.getHintOptions().get(PinotHintOptions.TABLE_HINT_OPTIONS));
    if (tableScanMetadata.getTimeBoundaryInfo() != null) {
      fragmentMetadata.setTimeBoundaryInfo(tableScanMetadata.getTimeBoundaryInfo());
    }
  }

  private PlanFragment createFragment(int fragmentId, PlanNode planNode, List<PlanFragment> inputFragments,
      Context context, List<String> workers) {
    // track new plan fragment
    PlanFragment fragment = new PlanFragment(fragmentId, planNode, inputFragments);
    context._planFragmentMap.put(fragmentId, fragment);
    // add fragment metadata
    DispatchablePlanMetadata fragmentMetadata = new DispatchablePlanMetadata();
    fragmentMetadata.setWorkerIdToServerInstanceMap(createWorkerMap(workers, context));
    context._fragmentMetadataMap.put(fragmentId, fragmentMetadata);
    return fragment;
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
      case RANDOM_EXCHANGE:
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
    // IMP: Return mailbox info sorted by workerIds. This is because SendingMailbox are created in this order, and
    // record assignment for hash exchange follows modulo arithmetic. e.g. if we have sending mailbox in order:
    // [worker-1, worker-0], then records with modulo 0 hash would end up in worker-1.
    // Note that the workerIds list will be >1 in length only when there's a parallelism change. It's important to
    // also know that MailboxSendOperator will iterate over this List<MailboxInfo> in order, and within each iteration
    // iterate over all the workerIds of that MailboxInfo. The result List<SendingMailbox> is used for modulo
    // arithmetic for any partitioning exchange strategy.
    result.sort(Comparator.comparingInt(info -> info.getWorkerIds().get(0)));
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
