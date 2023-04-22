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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.AbstractStageNode;
import org.apache.pinot.query.planner.stage.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.WorkerManager;
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
    distributedStagePlan.setServer(stringToInstance(stagePlan.getInstanceId()));
    distributedStagePlan.setStageRoot(StageNodeSerDeUtils.deserializeStageNode(stagePlan.getStageRoot()));
    Map<Integer, Worker.StageMetadata> metadataMap = stagePlan.getStageMetadataMap();
    distributedStagePlan.getMetadataMap().putAll(protoMapToStageMetadataMap(metadataMap));
    return distributedStagePlan;
  }

  public static Worker.StagePlan serialize(DistributedStagePlan distributedStagePlan) {
    return Worker.StagePlan.newBuilder()
        .setStageId(distributedStagePlan.getStageId())
        .setInstanceId(instanceToString(distributedStagePlan.getServer()))
        .setStageRoot(StageNodeSerDeUtils.serializeStageNode((AbstractStageNode) distributedStagePlan.getStageRoot()))
        .putAllStageMetadata(stageMetadataMapToProtoMap(distributedStagePlan.getMetadataMap())).build();
  }

  private static final Pattern VIRTUAL_SERVER_PATTERN = Pattern.compile(
      "(?<virtualid>[0-9]+)@(?<host>[^:]+):(?<port>[0-9]+)\\((?<grpc>[0-9]+):(?<service>[0-9]+):(?<mailbox>[0-9]+)\\)");

  public static VirtualServer stringToInstance(String serverInstanceString) {
    Matcher matcher = VIRTUAL_SERVER_PATTERN.matcher(serverInstanceString);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("Unexpected serverInstanceString '" + serverInstanceString + "'. This might "
          + "happen if you are upgrading from an old version of the multistage engine to the current one in a rolling "
          + "fashion.");
    }

    // Skipped netty and grpc port as they are not used in worker instance.
    return new VirtualServer(WorkerManager.getServerInstance(matcher.group("host"),
        Integer.parseInt(matcher.group("port")), Integer.parseInt(matcher.group("grpc")),
        Integer.parseInt(matcher.group("service")), Integer.parseInt(matcher.group("mailbox"))),
        Integer.parseInt(matcher.group("virtualid")));
  }

  public static String instanceToString(VirtualServer serverInstance) {
    return String.format("%s@%s:%s(%s:%s:%s)", serverInstance.getVirtualId(), serverInstance.getHostname(),
        serverInstance.getPort(), serverInstance.getGrpcPort(), serverInstance.getQueryServicePort(),
        serverInstance.getQueryMailboxPort());
  }

  public static Map<Integer, StageMetadata> protoMapToStageMetadataMap(Map<Integer, Worker.StageMetadata> protoMap) {
    Map<Integer, StageMetadata> metadataMap = new HashMap<>();
    for (Map.Entry<Integer, Worker.StageMetadata> e : protoMap.entrySet()) {
      metadataMap.put(e.getKey(), fromWorkerStageMetadata(e.getValue()));
    }
    return metadataMap;
  }

  private static StageMetadata fromWorkerStageMetadata(Worker.StageMetadata workerStageMetadata) {
    StageMetadata stageMetadata = new StageMetadata();
    // scanned table
    stageMetadata.getScannedTables().addAll(workerStageMetadata.getDataSourcesList());
    // server instance to table-segments mapping
    for (String serverInstanceString : workerStageMetadata.getInstancesList()) {
      stageMetadata.getServerInstances().add(stringToInstance(serverInstanceString));
    }
    for (Map.Entry<String, Worker.SegmentMap> instanceEntry
        : workerStageMetadata.getInstanceToSegmentMapMap().entrySet()) {
      Map<String, List<String>> tableToSegmentMap = new HashMap<>();
      for (Map.Entry<String, Worker.SegmentList> tableEntry
          : instanceEntry.getValue().getTableTypeToSegmentListMap().entrySet()) {
        tableToSegmentMap.put(tableEntry.getKey(), tableEntry.getValue().getSegmentsList());
      }
      stageMetadata.getServerInstanceToSegmentsMap()
          .put(stringToInstance(instanceEntry.getKey()).getServer(), tableToSegmentMap);
    }
    // time boundary info
    if (!workerStageMetadata.getTimeColumn().isEmpty()) {
      stageMetadata.setTimeBoundaryInfo(new TimeBoundaryInfo(workerStageMetadata.getTimeColumn(),
          workerStageMetadata.getTimeValue()));
    }
    return stageMetadata;
  }

  public static Map<Integer, Worker.StageMetadata> stageMetadataMapToProtoMap(Map<Integer, StageMetadata> metadataMap) {
    Map<Integer, Worker.StageMetadata> protoMap = new HashMap<>();
    for (Map.Entry<Integer, StageMetadata> e : metadataMap.entrySet()) {
      protoMap.put(e.getKey(), toWorkerStageMetadata(e.getValue()));
    }
    return protoMap;
  }

  private static Worker.StageMetadata toWorkerStageMetadata(StageMetadata stageMetadata) {
    Worker.StageMetadata.Builder builder = Worker.StageMetadata.newBuilder();
    // scanned table
    builder.addAllDataSources(stageMetadata.getScannedTables());
    // server instance to table-segments mapping
    for (VirtualServer serverInstance : stageMetadata.getServerInstances()) {
      builder.addInstances(instanceToString(serverInstance));
    }
    for (Map.Entry<ServerInstance, Map<String, List<String>>> instanceEntry
        : stageMetadata.getServerInstanceToSegmentsMap().entrySet()) {
      Map<String, Worker.SegmentList> tableToSegmentMap = new HashMap<>();
      for (Map.Entry<String, List<String>> tableEntry : instanceEntry.getValue().entrySet()) {
        tableToSegmentMap.put(tableEntry.getKey(),
            Worker.SegmentList.newBuilder().addAllSegments(tableEntry.getValue()).build());
      }
      builder.putInstanceToSegmentMap(instanceToString(new VirtualServer(instanceEntry.getKey(), 0)),
          Worker.SegmentMap.newBuilder().putAllTableTypeToSegmentList(tableToSegmentMap).build());
    }
    // time boundary info
    if (stageMetadata.getTimeBoundaryInfo() != null) {
      builder.setTimeColumn(stageMetadata.getTimeBoundaryInfo().getTimeColumn());
      builder.setTimeValue(stageMetadata.getTimeBoundaryInfo().getTimeValue());
    }
    return builder.build();
  }
}
