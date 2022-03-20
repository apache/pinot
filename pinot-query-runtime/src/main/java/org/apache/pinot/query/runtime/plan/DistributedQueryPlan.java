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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.planner.StageMetadata;
import org.apache.pinot.query.planner.nodes.StageNode;


/**
 * WorkerQueryRequest is the extended version of the {@link org.apache.pinot.core.query.request.ServerQueryRequest}.
 */
public class DistributedQueryPlan implements Serializable {
  private final String _stageId;
  private ServerInstance _serverInstance;
  private StageNode _stageRoot;
  private Map<String, StageMetadata> _metadataMap;

  public DistributedQueryPlan(String stageId) {
    _stageId = stageId;
    _metadataMap = new HashMap<>();
  }

  public DistributedQueryPlan(String stageId, ServerInstance serverInstance, StageNode stageRoot,
      Map<String, StageMetadata> metadataMap) {
    _stageId = stageId;
    _serverInstance = serverInstance;
    _stageRoot = stageRoot;
    _metadataMap = metadataMap;
  }

  public String getStageId() {
    return _stageId;
  }

  public ServerInstance getServerInstance() {
    return _serverInstance;
  }

  public StageNode getStageRoot() {
    return _stageRoot;
  }

  public Map<String, StageMetadata> getMetadataMap() {
    return _metadataMap;
  }

  public void setServerInstance(ServerInstance serverInstance) {
    _serverInstance = serverInstance;
  }

  public void setStageRoot(StageNode stageRoot) {
    _stageRoot = stageRoot;
  }
}
