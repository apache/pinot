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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.MailboxMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.plan.StageMetadata;
import org.apache.pinot.query.runtime.plan.StagePlan;


/**
 * This utility class serialize/deserialize between {@link Worker.StagePlan} elements to Planner elements.
 */
public class QueryPlanSerDeUtils {
  private QueryPlanSerDeUtils() {
  }

  public static StagePlan fromProtoStagePlan(Worker.StagePlan protoStagePlan)
      throws InvalidProtocolBufferException {
    AbstractPlanNode rootNode =
        StageNodeSerDeUtils.deserializeStageNode(Plan.StageNode.parseFrom(protoStagePlan.getRootNode()));
    StageMetadata stageMetadata = fromProtoStageMetadata(protoStagePlan.getStageMetadata());
    return new StagePlan(protoStagePlan.getStageId(), rootNode, stageMetadata);
  }

  private static StageMetadata fromProtoStageMetadata(Worker.StageMetadata protoStageMetadata)
      throws InvalidProtocolBufferException {
    List<WorkerMetadata> workerMetadataList =
        protoStageMetadata.getWorkerMetadataList().stream().map(QueryPlanSerDeUtils::fromProtoWorkerMetadata)
            .collect(Collectors.toList());
    Map<String, String> customProperties = fromProtoProperties(protoStageMetadata.getCustomProperty());
    return new StageMetadata(workerMetadataList, customProperties);
  }

  private static WorkerMetadata fromProtoWorkerMetadata(Worker.WorkerMetadata protoWorkerMetadata) {
    VirtualServerAddress virtualAddress = VirtualServerAddress.parse(protoWorkerMetadata.getVirtualAddress());
    Map<Integer, MailboxMetadata> mailboxMetadataMap = protoWorkerMetadata.getMailboxMetadataMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> fromProtoMailbox(e.getValue())));
    return new WorkerMetadata(virtualAddress, mailboxMetadataMap, protoWorkerMetadata.getCustomPropertyMap());
  }

  private static MailboxMetadata fromProtoMailbox(Worker.MailboxMetadata protoMailboxMetadata) {
    List<VirtualServerAddress> virtualAddresses =
        protoMailboxMetadata.getVirtualAddressList().stream().map(VirtualServerAddress::parse)
            .collect(Collectors.toList());
    return new MailboxMetadata(protoMailboxMetadata.getMailboxIdList(), virtualAddresses);
  }

  public static Map<String, String> fromProtoProperties(ByteString protoProperties)
      throws InvalidProtocolBufferException {
    return Worker.Properties.parseFrom(protoProperties).getPropertyMap();
  }

  public static List<Worker.WorkerMetadata> toProtoWorkerMetadataList(List<WorkerMetadata> workerMetadataList) {
    return workerMetadataList.stream().map(QueryPlanSerDeUtils::toProtoWorkerMetadata).collect(Collectors.toList());
  }

  private static Worker.WorkerMetadata toProtoWorkerMetadata(WorkerMetadata workerMetadata) {
    Map<Integer, Worker.MailboxMetadata> protoMailboxMetadataMap =
        workerMetadata.getMailboxMetadataMap().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> toProtoMailboxMetadata(e.getValue())));
    return Worker.WorkerMetadata.newBuilder().setVirtualAddress(workerMetadata.getVirtualAddress().toString())
        .putAllMailboxMetadata(protoMailboxMetadataMap).putAllCustomProperty(workerMetadata.getCustomProperties())
        .build();
  }

  private static Worker.MailboxMetadata toProtoMailboxMetadata(MailboxMetadata mailboxMetadata) {
    List<String> virtualAddresses =
        mailboxMetadata.getVirtualAddresses().stream().map(VirtualServerAddress::toString).collect(Collectors.toList());
    return Worker.MailboxMetadata.newBuilder().addAllMailboxId(mailboxMetadata.getMailboxIds())
        .addAllVirtualAddress(virtualAddresses).build();
  }

  public static ByteString toProtoProperties(Map<String, String> properties) {
    return Worker.Properties.newBuilder().putAllProperty(properties).build().toByteString();
  }
}
