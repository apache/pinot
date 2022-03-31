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
import org.apache.pinot.common.proto.Plan;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.routing.WorkerInstance;
import org.apache.pinot.query.runtime.plan.DistributedStagePlan;


/**
 * This utility class serialize/deserialize between {@link Plan.StagePlan} elements to Planner elements.
 */
public class QueryPlanSerDeUtils {

  private QueryPlanSerDeUtils() {
    // do not instantiate.
  }

  public static DistributedStagePlan deserialize(Plan.StagePlan stagePlan) {
    DistributedStagePlan distributedStagePlan = new DistributedStagePlan(stagePlan.getStageId());
    distributedStagePlan.setServerInstance(stringToInstance(stagePlan.getInstanceId()));
    distributedStagePlan.setStageRoot(StageNodeSerDeUtils.deserializeStageRoot(stagePlan.getSerializedStageRoot()));
    Map<Integer, StageMetadata> metadataMap = distributedStagePlan.getMetadataMap();
    return distributedStagePlan;
  }

  public static Plan.StagePlan serialize(DistributedStagePlan distributedStagePlan) {
    return Plan.StagePlan.newBuilder()
        .setInstanceId(instanceToString(distributedStagePlan.getServerInstance()))
        .setSerializedStageRoot(StageNodeSerDeUtils.serializeStageRoot(distributedStagePlan.getStageRoot()))
        .putAllStageMetadata(stageMetadataMapToProtoMap(distributedStagePlan.getMetadataMap())).build();
  }

  public static ServerInstance stringToInstance(String serverInstanceString) {
    String[] s = StringUtils.split(serverInstanceString, '_');
    return new WorkerInstance(s[0], Integer.parseInt(s[1]), Integer.parseInt(s[2]));
  }

  public static String instanceToString(ServerInstance serverInstance) {
    return StringUtils.join(serverInstance.getHostname(), '_', serverInstance.getPort(), '_',
        serverInstance.getGrpcPort());
  }

  public static Map<Integer, StageMetadata> getStageMetadataMap(Map<Integer, Plan.StageMetadata> protoMap) {
    Map<Integer, StageMetadata> metadataMap = new HashMap<>();
    for (Map.Entry<Integer, Plan.StageMetadata> e : protoMap.entrySet()) {
      metadataMap.put(e.getKey(), protoMapToStageMetadataMap(e.getValue()));
    }
    return metadataMap;
  }

  private static StageMetadata protoMapToStageMetadataMap(Plan.StageMetadata workerStageMetadata) {
    StageMetadata stageMetadata = new StageMetadata();
    stageMetadata.getScannedTables().addAll(workerStageMetadata.getDataSourcesList());
    for (String serverInstanceString : workerStageMetadata.getInstancesList()) {
      stageMetadata.getServerInstances().add(stringToInstance(serverInstanceString));
    }
    for (Map.Entry<String, Plan.SegmentList> e : workerStageMetadata.getInstanceToSegmentListMap().entrySet()) {
      stageMetadata.getServerInstanceToSegmentsMap().put(stringToInstance(e.getKey()), e.getValue().getSegmentsList());
    }
    return stageMetadata;
  }

  public static Map<Integer, Plan.StageMetadata> stageMetadataMapToProtoMap(Map<Integer, StageMetadata> metadataMap) {
    Map<Integer, Plan.StageMetadata> protoMap = new HashMap<>();
    for (Map.Entry<Integer, StageMetadata> e : metadataMap.entrySet()) {
      protoMap.put(e.getKey(), toWorkerStageMetadata(e.getValue()));
    }
    return protoMap;
  }

  private static Plan.StageMetadata toWorkerStageMetadata(StageMetadata stageMetadata) {
    Plan.StageMetadata.Builder builder = Plan.StageMetadata.newBuilder();
    builder.addAllDataSources(stageMetadata.getScannedTables());
    for (ServerInstance serverInstance : stageMetadata.getServerInstances()) {
      builder.addInstances(instanceToString(serverInstance));
    }
    for (Map.Entry<ServerInstance, List<String>> e : stageMetadata.getServerInstanceToSegmentsMap().entrySet()) {
      builder.putInstanceToSegmentList(instanceToString(e.getKey()),
          Plan.SegmentList.newBuilder().addAllSegments(e.getValue()).build());
    }
    return builder.build();
  }
}
