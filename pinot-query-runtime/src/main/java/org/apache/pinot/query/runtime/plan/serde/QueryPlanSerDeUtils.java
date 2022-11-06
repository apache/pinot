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
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.core.routing.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.stage.AbstractStageNode;
import org.apache.pinot.query.planner.stage.StageNodeSerDeUtils;
import org.apache.pinot.query.routing.WorkerInstance;
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
    distributedStagePlan.setServerInstance(stringToInstance(stagePlan.getInstanceId()));
    distributedStagePlan.setStageRoot(StageNodeSerDeUtils.deserializeStageNode(stagePlan.getStageRoot()));
    Map<Integer, Worker.StageMetadata> metadataMap = stagePlan.getStageMetadataMap();
    distributedStagePlan.getMetadataMap().putAll(protoMapToStageMetadataMap(metadataMap));
    return distributedStagePlan;
  }

  public static Worker.StagePlan serialize(DistributedStagePlan distributedStagePlan) {
    return Worker.StagePlan.newBuilder()
        .setStageId(distributedStagePlan.getStageId())
        .setInstanceId(instanceToString(distributedStagePlan.getServerInstance()))
        .setStageRoot(StageNodeSerDeUtils.serializeStageNode((AbstractStageNode) distributedStagePlan.getStageRoot()))
        .putAllStageMetadata(stageMetadataMapToProtoMap(distributedStagePlan.getMetadataMap())).build();
  }

  public static ServerInstance stringToInstance(String serverInstanceString) {
    String[] s = StringUtils.split(serverInstanceString, '_');
    // Skipped netty and grpc port as they are not used in worker instance.
    return new WorkerInstance(s[0], Integer.parseInt(s[1]), Integer.parseInt(s[2]), Integer.parseInt(s[3]),
        Integer.parseInt(s[4]));
  }

  public static String instanceToString(ServerInstance serverInstance) {
    return StringUtils.join(serverInstance.getHostname(), '_', serverInstance.getPort(), '_',
        serverInstance.getGrpcPort(), '_', serverInstance.getQueryServicePort(), '_',
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
      stageMetadata.getServerInstanceToSegmentsMap().put(stringToInstance(instanceEntry.getKey()), tableToSegmentMap);
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
    for (ServerInstance serverInstance : stageMetadata.getServerInstances()) {
      builder.addInstances(instanceToString(serverInstance));
    }
    for (Map.Entry<ServerInstance, Map<String, List<String>>> instanceEntry
        : stageMetadata.getServerInstanceToSegmentsMap().entrySet()) {
      Map<String, Worker.SegmentList> tableToSegmentMap = new HashMap<>();
      for (Map.Entry<String, List<String>> tableEntry : instanceEntry.getValue().entrySet()) {
        tableToSegmentMap.put(tableEntry.getKey(),
            Worker.SegmentList.newBuilder().addAllSegments(tableEntry.getValue()).build());
      }
      builder.putInstanceToSegmentMap(instanceToString(instanceEntry.getKey()),
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
