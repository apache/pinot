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
package org.apache.pinot.query.runtime.plan.serde;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.physical.DispatchablePlanFragment;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.QueryServerInstance;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;
import org.apache.pinot.query.runtime.plan.StageMetadata;


/**
 * This utility class serialize/deserialize between {@link Worker.StagePlan} elements to Planner elements.
 */
public class QueryPlanSerDeUtils {
  private static final Pattern VIRTUAL_SERVER_PATTERN =
      Pattern.compile("(?<virtualid>[0-9]+)@(?<host>[^:]+):(?<port>[0-9]+)");

  private QueryPlanSerDeUtils() {
    // do not instantiate.
  }

  public static List<DistributedStagePlan> deserializeStagePlan(Worker.QueryRequest request) {
    List<DistributedStagePlan> distributedStagePlans = new ArrayList<>();
    for (Worker.StagePlan stagePlan : request.getStagePlanList()) {
      distributedStagePlans.addAll(deserializeStagePlan(stagePlan));
    }
    return distributedStagePlans;
  }

  public static VirtualServerAddress protoToAddress(String virtualAddressStr) {
    Matcher matcher = VIRTUAL_SERVER_PATTERN.matcher(virtualAddressStr);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Unexpected virtualAddressStr '" + virtualAddressStr + "'. This might "
          + "happen if you are upgrading from an old version of the multistage engine to the current one in a rolling "
          + "fashion.");
    }

    // Skipped netty and grpc port as they are not used in worker instance.
    return new VirtualServerAddress(matcher.group("host"), Integer.parseInt(matcher.group("port")),
        Integer.parseInt(matcher.group("virtualid")));
  }

  public static String addressToProto(VirtualServerAddress serverAddress) {
    return String.format("%s@%s:%s", serverAddress.workerId(), serverAddress.hostname(), serverAddress.port());
  }

  private static List<DistributedStagePlan> deserializeStagePlan(Worker.StagePlan stagePlan) {
    List<DistributedStagePlan> distributedStagePlans = new ArrayList<>();
    String serverAddress = stagePlan.getStageMetadata().getServerAddress();
    String[] hostPort = StringUtils.split(serverAddress, ':');
    String hostname = hostPort[0];
    int port = Integer.parseInt(hostPort[1]);
    AbstractPlanNode stageRoot = StageNodeSerDeUtils.deserializeStageNode(stagePlan.getStageRoot());
    StageMetadata stageMetadata = fromProtoStageMetadata(stagePlan.getStageMetadata());
    for (int workerId : stagePlan.getStageMetadata().getWorkerIdsList()) {
      DistributedStagePlan distributedStagePlan = new DistributedStagePlan(stagePlan.getStageId());
      VirtualServerAddress virtualServerAddress = new VirtualServerAddress(hostname, port, workerId);
      distributedStagePlan.setServer(virtualServerAddress);
      distributedStagePlan.setStageRoot(stageRoot);
      distributedStagePlan.setStageMetadata(stageMetadata);
      distributedStagePlans.add(distributedStagePlan);
    }
    return distributedStagePlans;
  }

  private static StageMetadata fromProtoStageMetadata(Worker.StageMetadata protoStageMetadata) {
    StageMetadata.Builder builder = new StageMetadata.Builder();
    List<WorkerMetadata> workerMetadataList = new ArrayList<>();
    for (Worker.WorkerMetadata protoWorkerMetadata : protoStageMetadata.getWorkerMetadataList()) {
      workerMetadataList.add(fromProtoWorkerMetadata(protoWorkerMetadata));
    }
    builder.setWorkerMetadataList(workerMetadataList);
    builder.putAllCustomProperties(protoStageMetadata.getCustomPropertyMap());
    return builder.build();
  }

  private static WorkerMetadata fromProtoWorkerMetadata(Worker.WorkerMetadata protoWorkerMetadata) {
    WorkerMetadata.Builder builder = new WorkerMetadata.Builder();
    builder.setVirtualServerAddress(protoToAddress(protoWorkerMetadata.getVirtualAddress()));
    builder.putAllMailBoxInfosMap(fromProtoMailboxMetadataMap(protoWorkerMetadata.getMailboxMetadataMap()));
    builder.putAllCustomProperties(protoWorkerMetadata.getCustomPropertyMap());
    return builder.build();
  }

  private static Map<Integer, MailboxMetadata> fromProtoMailboxMetadataMap(
      Map<Integer, Worker.MailboxMetadata> mailboxMetadataMap) {
    Map<Integer, MailboxMetadata> mailboxMap = new HashMap<>();
    for (Map.Entry<Integer, Worker.MailboxMetadata> entry : mailboxMetadataMap.entrySet()) {
      mailboxMap.put(entry.getKey(), fromProtoMailbox(entry.getValue()));
    }
    return mailboxMap;
  }

  private static MailboxMetadata fromProtoMailbox(Worker.MailboxMetadata protoMailboxMetadata) {
    List<String> mailboxIds = new ArrayList<>();
    List<VirtualServerAddress> virtualAddresses = new ArrayList<>();
    for (int i = 0; i < protoMailboxMetadata.getMailboxIdCount(); i++) {
      mailboxIds.add(protoMailboxMetadata.getMailboxId(i));
      virtualAddresses.add(protoToAddress(protoMailboxMetadata.getVirtualAddress(i)));
    }
    MailboxMetadata mailboxMetadata =
        new MailboxMetadata(mailboxIds, virtualAddresses, protoMailboxMetadata.getCustomPropertyMap());
    return mailboxMetadata;
  }

  public static Worker.StageMetadata toProtoStageMetadata(List<Worker.WorkerMetadata> workerMetadataList,
      Map<String, String> customProperties, QueryServerInstance serverInstance, List<Integer> workerIds) {
    return Worker.StageMetadata.newBuilder().addAllWorkerMetadata(workerMetadataList)
        .putAllCustomProperty(customProperties)
        .setServerAddress(String.format("%s:%d", serverInstance.getHostname(), serverInstance.getQueryMailboxPort()))
        .addAllWorkerIds(workerIds).build();
  }

  public static List<Worker.WorkerMetadata> toProtoWorkerMetadataList(DispatchablePlanFragment planFragment) {
    List<WorkerMetadata> workerMetadataList = planFragment.getWorkerMetadataList();
    List<Worker.WorkerMetadata> protoWorkerMetadataList = new ArrayList<>(workerMetadataList.size());
    for (WorkerMetadata workerMetadata : workerMetadataList) {
      protoWorkerMetadataList.add(toProtoWorkerMetadata(workerMetadata));
    }
    return protoWorkerMetadataList;
  }

  private static Worker.WorkerMetadata toProtoWorkerMetadata(WorkerMetadata workerMetadata) {
    Worker.WorkerMetadata.Builder builder = Worker.WorkerMetadata.newBuilder();
    builder.setVirtualAddress(addressToProto(workerMetadata.getVirtualServerAddress()));
    builder.putAllMailboxMetadata(toProtoMailboxMap(workerMetadata.getMailBoxInfosMap()));
    builder.putAllCustomProperty(workerMetadata.getCustomProperties());
    return builder.build();
  }

  private static Map<Integer, Worker.MailboxMetadata> toProtoMailboxMap(Map<Integer, MailboxMetadata> mailBoxInfosMap) {
    Map<Integer, Worker.MailboxMetadata> mailboxMetadataMap = new HashMap<>();
    for (Map.Entry<Integer, MailboxMetadata> entry : mailBoxInfosMap.entrySet()) {
      mailboxMetadataMap.put(entry.getKey(), toProtoMailbox(entry.getValue()));
    }
    return mailboxMetadataMap;
  }

  private static Worker.MailboxMetadata toProtoMailbox(MailboxMetadata mailboxMetadata) {
    Worker.MailboxMetadata.Builder builder = Worker.MailboxMetadata.newBuilder();
    for (int i = 0; i < mailboxMetadata.getMailBoxIdList().size(); i++) {
      builder.addMailboxId(mailboxMetadata.getMailBoxId(i));
      builder.addVirtualAddress(mailboxMetadata.getVirtualAddress(i).toString());
    }
    builder.putAllCustomProperty(mailboxMetadata.getCustomProperties());
    return builder.build();
  }
}
