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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.planner.serde.PlanNodeDeserializer;
import org.apache.pinot.spi.utils.JsonUtils;


/// This utility class serialize/deserialize between [Worker.StagePlan] elements to Planner elements.
///
/// The leaf-stage segment maps of a [WorkerMetadata] have two wire encodings, picked per request by the broker:
///
/// - **proto**: the native `tableSegmentsMap` / `logicalTableSegmentsMap` fields of [Worker.WorkerMetadata].
/// - **legacy JSON**: a JSON string under the [WorkerMetadata#TABLE_SEGMENTS_MAP_KEY] /
///   [WorkerMetadata#LOGICAL_TABLE_SEGMENTS_MAP_KEY] custom property, which is all that servers predating the proto
///   fields understand.
///
/// Decoding accepts both, so a server always understands every broker; the proto encoding is off until an operator
/// turns it on, which they only do once every server understands it (see
/// `CommonConstants.Broker.CONFIG_OF_MSE_ENABLE_PROTO_SEGMENT_LIST`).
public class QueryPlanSerDeUtils {
  private QueryPlanSerDeUtils() {
  }

  public static StagePlan fromProtoStagePlan(Worker.StagePlan protoStagePlan)
      throws InvalidProtocolBufferException {
    PlanNode rootNode = PlanNodeDeserializer.process(Plan.PlanNode.parseFrom(protoStagePlan.getRootNode()));
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

  @VisibleForTesting
  static WorkerMetadata fromProtoWorkerMetadata(Worker.WorkerMetadata protoWorkerMetadata)
      throws InvalidProtocolBufferException {
    Map<Integer, ByteString> protoMailboxInfosMap = protoWorkerMetadata.getMailboxInfosMap();
    Map<Integer, MailboxInfos> mailboxInfosMap = Maps.newHashMapWithExpectedSize(protoMailboxInfosMap.size());
    for (Map.Entry<Integer, ByteString> entry : protoMailboxInfosMap.entrySet()) {
      mailboxInfosMap.put(entry.getKey(), fromProtoMailboxInfos(entry.getValue()));
    }
    // A broker using the legacy encoding ships the segment maps as JSON custom properties. Move them out of the custom
    // properties into WorkerMetadata unparsed: each worker parses its own list on first access, on its own thread,
    // instead of every worker of the stage being parsed here one after another. The custom properties stay
    // unmodifiable either way, as the proto map view is, so that no server path can come to depend on writing to them
    // under one encoding only.
    Map<String, String> customProperties = protoWorkerMetadata.getCustomPropertyMap();
    String tableSegmentsJson = customProperties.get(WorkerMetadata.TABLE_SEGMENTS_MAP_KEY);
    String logicalTableSegmentsJson = customProperties.get(WorkerMetadata.LOGICAL_TABLE_SEGMENTS_MAP_KEY);
    if (tableSegmentsJson != null || logicalTableSegmentsJson != null) {
      Map<String, String> strippedProperties = new HashMap<>(customProperties);
      strippedProperties.remove(WorkerMetadata.TABLE_SEGMENTS_MAP_KEY);
      strippedProperties.remove(WorkerMetadata.LOGICAL_TABLE_SEGMENTS_MAP_KEY);
      customProperties = Collections.unmodifiableMap(strippedProperties);
    }
    WorkerMetadata workerMetadata =
        new WorkerMetadata(protoWorkerMetadata.getWorkedId(), mailboxInfosMap, customProperties);
    if (protoWorkerMetadata.hasTableSegmentsMap()) {
      workerMetadata.setTableSegmentsMap(fromProtoSegmentsMap(protoWorkerMetadata.getTableSegmentsMap()));
    } else if (tableSegmentsJson != null) {
      workerMetadata.setTableSegmentsMapJson(tableSegmentsJson);
    }
    if (protoWorkerMetadata.hasLogicalTableSegmentsMap()) {
      workerMetadata.setLogicalTableSegmentsMap(
          fromProtoSegmentsMap(protoWorkerMetadata.getLogicalTableSegmentsMap()));
    } else if (logicalTableSegmentsJson != null) {
      workerMetadata.setLogicalTableSegmentsMapJson(logicalTableSegmentsJson);
    }
    return workerMetadata;
  }

  private static Map<String, List<String>> fromProtoSegmentsMap(Worker.SegmentsMap protoSegmentsMap) {
    Map<String, Worker.SegmentList> protoSegments = protoSegmentsMap.getSegmentsMap();
    Map<String, List<String>> segmentsMap = Maps.newHashMapWithExpectedSize(protoSegments.size());
    for (Map.Entry<String, Worker.SegmentList> entry : protoSegments.entrySet()) {
      segmentsMap.put(entry.getKey(), new ArrayList<>(entry.getValue().getSegmentList()));
    }
    return segmentsMap;
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

  /// Encodes the worker metadata for the wire with the leaf-stage segment maps in the legacy JSON encoding, which every
  /// server understands. Kept for callers that predate the proto encoding.
  public static List<Worker.WorkerMetadata> toProtoWorkerMetadataList(List<WorkerMetadata> workerMetadataList) {
    return toProtoWorkerMetadataList(workerMetadataList, false);
  }

  /// Encodes the worker metadata for the wire, with the leaf-stage segment maps as native proto fields when
  /// `protoSegmentList` is set and as legacy JSON custom properties otherwise (see the class documentation).
  public static List<Worker.WorkerMetadata> toProtoWorkerMetadataList(List<WorkerMetadata> workerMetadataList,
      boolean protoSegmentList) {
    List<Worker.WorkerMetadata> protoWorkerMetadataList = new ArrayList<>(workerMetadataList.size());
    for (WorkerMetadata workerMetadata : workerMetadataList) {
      protoWorkerMetadataList.add(toProtoWorkerMetadata(workerMetadata, protoSegmentList));
    }
    return protoWorkerMetadataList;
  }

  private static Worker.WorkerMetadata toProtoWorkerMetadata(WorkerMetadata workerMetadata,
      boolean protoSegmentList) {
    Worker.WorkerMetadata.Builder builder = Worker.WorkerMetadata.newBuilder()
        .setWorkedId(workerMetadata.getWorkerId())
        .putAllCustomProperty(workerMetadata.getCustomProperties());
    for (Map.Entry<Integer, MailboxInfos> entry : workerMetadata.getMailboxInfosMap().entrySet()) {
      builder.putMailboxInfos(entry.getKey(), entry.getValue().toProtoBytes());
    }
    Map<String, List<String>> tableSegmentsMap = workerMetadata.getTableSegmentsMap();
    if (tableSegmentsMap != null) {
      if (protoSegmentList) {
        builder.setTableSegmentsMap(toProtoSegmentsMap(tableSegmentsMap));
      } else {
        builder.putCustomProperty(WorkerMetadata.TABLE_SEGMENTS_MAP_KEY, encodeSegmentsMapJson(tableSegmentsMap));
      }
    }
    Map<String, List<String>> logicalTableSegmentsMap = workerMetadata.getLogicalTableSegmentsMap();
    if (logicalTableSegmentsMap != null) {
      if (protoSegmentList) {
        builder.setLogicalTableSegmentsMap(toProtoSegmentsMap(logicalTableSegmentsMap));
      } else {
        builder.putCustomProperty(WorkerMetadata.LOGICAL_TABLE_SEGMENTS_MAP_KEY,
            encodeSegmentsMapJson(logicalTableSegmentsMap));
      }
    }
    return builder.build();
  }

  private static Worker.SegmentsMap toProtoSegmentsMap(Map<String, List<String>> segmentsMap) {
    Worker.SegmentsMap.Builder builder = Worker.SegmentsMap.newBuilder();
    for (Map.Entry<String, List<String>> entry : segmentsMap.entrySet()) {
      builder.putSegments(entry.getKey(), Worker.SegmentList.newBuilder().addAllSegment(entry.getValue()).build());
    }
    return builder.build();
  }

  /// JSON-encodes a segments map as `{"OFFLINE":["seg1","seg2"]}` for the legacy encoding.
  private static String encodeSegmentsMapJson(Map<String, List<String>> segmentsMap) {
    try {
      return JsonUtils.objectToString(segmentsMap);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Unable to serialize segments map: " + segmentsMap, e);
    }
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
