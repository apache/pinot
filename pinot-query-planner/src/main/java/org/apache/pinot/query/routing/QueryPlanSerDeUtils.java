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
package org.apache.pinot.query.routing;

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.plannode.AbstractPlanNode;
import org.apache.pinot.query.planner.plannode.StageNodeSerDeUtils;


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
    return new StagePlan(rootNode, stageMetadata);
  }

  private static StageMetadata fromProtoStageMetadata(Worker.StageMetadata protoStageMetadata)
      throws InvalidProtocolBufferException {
    List<Worker.WorkerMetadata> protoWorkerMetadataList = protoStageMetadata.getWorkerMetadataList();
    List<WorkerMetadata> workerMetadataList = new ArrayList<>(protoWorkerMetadataList.size());
    for (Worker.WorkerMetadata protoWorkerMetadata : protoWorkerMetadataList) {
      workerMetadataList.add(fromProtoWorkerMetadata(protoWorkerMetadata));
    }
    Map<String, String> customProperties = fromProtoProperties(protoStageMetadata.getCustomProperty());
    return new StageMetadata(protoStageMetadata.getStageId(), workerMetadataList, customProperties);
  }

  private static WorkerMetadata fromProtoWorkerMetadata(Worker.WorkerMetadata protoWorkerMetadata)
      throws InvalidProtocolBufferException {
    Map<Integer, ByteString> protoMailboxInfosMap = protoWorkerMetadata.getMailboxInfosMap();
    Map<Integer, MailboxInfos> mailboxInfosMap = Maps.newHashMapWithExpectedSize(protoMailboxInfosMap.size());
    for (Map.Entry<Integer, ByteString> entry : protoMailboxInfosMap.entrySet()) {
      mailboxInfosMap.put(entry.getKey(), fromProtoMailboxInfos(entry.getValue()));
    }
    return new WorkerMetadata(protoWorkerMetadata.getWorkedId(), mailboxInfosMap,
        protoWorkerMetadata.getCustomPropertyMap());
  }

  private static MailboxInfos fromProtoMailboxInfos(ByteString protoMailboxInfos)
      throws InvalidProtocolBufferException {
    return new MailboxInfos(Worker.MailboxInfos.parseFrom(protoMailboxInfos).getMailboxInfoList().stream()
        .map(v -> new MailboxInfo(v.getHostname(), v.getPort(), v.getWorkerIdList())).collect(Collectors.toList()));
  }

  public static Map<String, String> fromProtoProperties(ByteString protoProperties)
      throws InvalidProtocolBufferException {
    return Worker.Properties.parseFrom(protoProperties).getPropertyMap();
  }

  public static List<Worker.WorkerMetadata> toProtoWorkerMetadataList(List<WorkerMetadata> workerMetadataList) {
    return workerMetadataList.stream().map(QueryPlanSerDeUtils::toProtoWorkerMetadata).collect(Collectors.toList());
  }

  private static Worker.WorkerMetadata toProtoWorkerMetadata(WorkerMetadata workerMetadata) {
    Map<Integer, ByteString> mailboxInfosMap = workerMetadata.getMailboxInfosMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toProtoBytes()));
    return Worker.WorkerMetadata.newBuilder().setWorkedId(workerMetadata.getWorkerId())
        .putAllMailboxInfos(mailboxInfosMap).putAllCustomProperty(workerMetadata.getCustomProperties()).build();
  }

  public static Worker.MailboxInfos toProtoMailboxInfos(List<MailboxInfo> mailboxInfos) {
    List<Worker.MailboxInfo> protoMailboxInfos = mailboxInfos.stream().map(
        v -> Worker.MailboxInfo.newBuilder().setHostname(v.getHostname()).setPort(v.getPort())
            .addAllWorkerId(v.getWorkerIds()).build()).collect(Collectors.toList());
    return Worker.MailboxInfos.newBuilder().addAllMailboxInfo(protoMailboxInfos).build();
  }

  public static ByteString toProtoProperties(Map<String, String> properties) {
    return Worker.Properties.newBuilder().putAllProperty(properties).build().toByteString();
  }
}
