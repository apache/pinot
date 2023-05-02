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
import org.apache.pinot.query.planner.stage.AbstractStageNode;
import org.apache.pinot.query.planner.stage.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.StageMetadata;
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
        .setStageRoot(StageNodeSerDeUtils.serializeStageNode((AbstractStageNode) distributedStagePlan.getStageRoot()))
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

  private static StageMetadata fromProtoStageMetadata(Worker.StageMetadata protoStageMetadata) {
    StageMetadata.Builder builder = new StageMetadata.Builder();
    List<WorkerMetadata> workerMetadataList = new ArrayList<>();
    for (Worker.WorkerMetadata protoWorkerMetadata : protoStageMetadata.getWorkerMetadataList()) {
      workerMetadataList.add(fromProtoWorkerMetadata(protoWorkerMetadata));
    }
    builder.setWorkerMetadataList(workerMetadataList);
    builder.putAllCustomProperties(protoStageMetadata.getCustomPropertyMap());
    Map<Integer, List<MailboxInfo>> mailboxInfoMap = new HashMap<>();
    for (Map.Entry<Integer, Worker.StageMailboxInfo> entry : protoStageMetadata.getStageMailboxInfoMapMap()
        .entrySet()) {
      List<MailboxInfo> mailboxInfoList = new ArrayList<>();
      for (Worker.MailboxInfo protoMailboxInfo : entry.getValue().getMailboxInfoList()) {
        mailboxInfoList.add(new MailboxInfo(protoMailboxInfo.getMailboxId(), protoMailboxInfo.getMailboxType(),
            protoMailboxInfo.getMailboxHost(), protoMailboxInfo.getMailboxPort(),
            protoMailboxInfo.getCustomPropertyMap()));
      }
      mailboxInfoMap.put(entry.getKey(), mailboxInfoList);
    }
    builder.setMailBoxInfosMap(mailboxInfoMap);
    return builder.build();
  }

  private static WorkerMetadata fromProtoWorkerMetadata(Worker.WorkerMetadata protoWorkerMetadata) {
    WorkerMetadata.Builder builder = new WorkerMetadata.Builder();
    builder.setVirtualServerAddress(protoToAddress(protoWorkerMetadata.getVirtualAddress()));
    builder.putAllCustomProperties(protoWorkerMetadata.getCustomPropertyMap());
    return builder.build();
  }

  private static Worker.StageMetadata toProtoStageMetadata(StageMetadata stageMetadata) {
    Worker.StageMetadata.Builder builder = Worker.StageMetadata.newBuilder();
    for (WorkerMetadata workerMetadata : stageMetadata.getWorkerMetadataList()) {
      builder.addWorkerMetadata(toProtoWorkerMetadata(workerMetadata));
    }
    builder.putAllCustomProperty(stageMetadata.getCustomProperties());
    return builder.build();
  }

  private static Worker.WorkerMetadata toProtoWorkerMetadata(WorkerMetadata workerMetadata) {
    Worker.WorkerMetadata.Builder builder = Worker.WorkerMetadata.newBuilder();
    builder.setVirtualAddress(addressToProto(workerMetadata.getVirtualServerAddress()));
    builder.putAllCustomProperty(workerMetadata.getCustomProperties());
    return builder.build();
  }
}
