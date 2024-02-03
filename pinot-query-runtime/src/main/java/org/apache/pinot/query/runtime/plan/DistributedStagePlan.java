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
package org.apache.pinot.query.runtime.plan;

import java.util.List;
import java.util.Map;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;


/**
 * {@code DistributedStagePlan} is the deserialized version of the
 * {@link org.apache.pinot.common.proto.Worker.StagePlan}.
 *
 * <p>It is also the extended version of the {@link org.apache.pinot.core.query.request.ServerQueryRequest}.
 */
public class DistributedStagePlan {
  private final int _stageId;
  private final VirtualServerAddress _server;
  private final PlanNode _stageRoot;
  private final StageMetadata _stageMetadata;

  public DistributedStagePlan(int stageId, VirtualServerAddress server, PlanNode stageRoot,
      StageMetadata stageMetadata) {
    _stageId = stageId;
    _server = server;
    _stageRoot = stageRoot;
    _stageMetadata = stageMetadata;
  }

  public int getStageId() {
    return _stageId;
  }

  public VirtualServerAddress getServer() {
    return _server;
  }

  public PlanNode getStageRoot() {
    return _stageRoot;
  }

  public StageMetadata getStageMetadata() {
    return _stageMetadata;
  }

  public WorkerMetadata getCurrentWorkerMetadata() {
    return _stageMetadata.getWorkerMetadataList().get(_server.workerId());
  }

  public static boolean isLeafStage(DistributedStagePlan distributedStagePlan) {
    WorkerMetadata workerMetadata = distributedStagePlan.getCurrentWorkerMetadata();
    Map<String, List<String>> segments = WorkerMetadata.getTableSegmentsMap(workerMetadata);
    return segments != null && segments.size() > 0;
  }
}
