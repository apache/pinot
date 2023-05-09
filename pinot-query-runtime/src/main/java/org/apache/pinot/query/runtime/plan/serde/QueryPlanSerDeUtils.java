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
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.PlanFragmentMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;


/**
 * This utility class serialize/deserialize between {@link Worker.StagePlan} elements to Planner elements.
 */
public class QueryPlanSerDeUtils {

  private QueryPlanSerDeUtils() {
    // do not instantiate.
  }

  public static DistributedStagePlan deserialize(Worker.StagePlan stagePlan) {
    DistributedStagePlan distributedStagePlan = new DistributedStagePlan(stagePlan.getStageId());
    distributedStagePlan.setServer(protoToAddress(stagePlan.getVirtualAddress()));
    distributedStagePlan.setStageRoot(StageNodeSerDeUtils.deserializeStageNode(stagePlan.getStageRoot()));
    distributedStagePlan.setStageMetadata(fromProtoStageMetadata(stagePlan.getStageMetadata()));
    return distributedStagePlan;
  }

  public static Worker.StagePlan serialize(DistributedStagePlan distributedStagePlan) {
    return Worker.StagePlan.newBuilder()
        .setStageId(distributedStagePlan.getStageId())
        .setVirtualAddress(addressToProto(distributedStagePlan.getServer()))
        .setStageRoot(StageNodeSerDeUtils.serializeStageNode((AbstractPlanNode) distributedStagePlan.getStageRoot()))
        .setStageMetadata(toProtoStageMetadata(distributedStagePlan.getStageMetadata())).build();
  }

  private static final Pattern VIRTUAL_SERVER_PATTERN = Pattern.compile(
      "(?<virtualid>[0-9]+)@(?<host>[^:]+):(?<port>[0-9]+)");

  public static VirtualServerAddress protoToAddress(String virtualAddressStr) {
    Matcher matcher = VIRTUAL_SERVER_PATTERN.matcher(virtualAddressStr);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Unexpected virtualAddressStr '" + virtualAddressStr + "'. This might "
          + "happen if you are upgrading from an old version of the multistage engine to the current one in a rolling "
          + "fashion.");
    }

    // Skipped netty and grpc port as they are not used in worker instance.
    return new VirtualServerAddress(matcher.group("host"),
        Integer.parseInt(matcher.group("port")), Integer.parseInt(matcher.group("virtualid")));
  }

  public static String addressToProto(VirtualServerAddress serverAddress) {
    return String.format("%s@%s:%s", serverAddress.workerId(), serverAddress.hostname(), serverAddress.port());
  }

  private static PlanFragmentMetadata fromProtoStageMetadata(Worker.StageMetadata protoStageMetadata) {
    PlanFragmentMetadata.Builder builder = new PlanFragmentMetadata.Builder();
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

  private static Worker.StageMetadata toProtoStageMetadata(PlanFragmentMetadata planFragmentMetadata) {
    Worker.StageMetadata.Builder builder = Worker.StageMetadata.newBuilder();
    for (WorkerMetadata workerMetadata : planFragmentMetadata.getWorkerMetadataList()) {
      builder.addWorkerMetadata(toProtoWorkerMetadata(workerMetadata));
    }
    builder.putAllCustomProperty(planFragmentMetadata.getCustomProperties());
    return builder.build();
  }

  private static Worker.WorkerMetadata toProtoWorkerMetadata(WorkerMetadata workerMetadata) {
    Worker.WorkerMetadata.Builder builder = Worker.WorkerMetadata.newBuilder();
    builder.setVirtualAddress(addressToProto(workerMetadata.getVirtualServerAddress()));
    builder.putAllMailboxMetadata(toProtoMailboxMap(workerMetadata.getMailBoxInfosMap()));
    builder.putAllCustomProperty(workerMetadata.getCustomProperties());
    return builder.build();
  }

  private static Map<Integer, Worker.MailboxMetadata> toProtoMailboxMap(
      Map<Integer, MailboxMetadata> mailBoxInfosMap) {
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
