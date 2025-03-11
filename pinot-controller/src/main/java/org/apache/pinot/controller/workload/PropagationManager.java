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
package org.apache.pinot.controller.workload;

import java.util.Map;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;
import org.apache.pinot.spi.config.workload.QueryWorkloadConfig;


/**
 * The PropagationManager class is responsible for propagating the query workload refresh message to the relevant
 * instances based on the node configurations.
 */
public class PropagationManager {

  private final TablePropagationStrategy _tablePropagationStrategy;
  private final TenantPropagationStrategy _tenantPropagationStrategy;

  public PropagationManager(PinotHelixResourceManager pinotHelixResourceManager) {
    _tablePropagationStrategy = new TablePropagationStrategy(pinotHelixResourceManager);
    _tenantPropagationStrategy = new TenantPropagationStrategy(pinotHelixResourceManager);
  }

  public void propagate(QueryWorkloadConfig queryWorkloadConfig) {
    QueryWorkloadRefreshMessage refreshMessage = new QueryWorkloadRefreshMessage(queryWorkloadConfig);
    Map<NodeConfig.Type, NodeConfig> nodeConfigs = queryWorkloadConfig.getNodeConfigs();
    for (Map.Entry<NodeConfig.Type, NodeConfig> entry : nodeConfigs.entrySet()) {
      NodeConfig.Type nodeType = entry.getKey();
      NodeConfig nodeConfig = entry.getValue();
      PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
      switch (propagationType) {
        case TABLE:
          _tablePropagationStrategy.propagate(refreshMessage, nodeType, nodeConfig);
          break;
        case TENANT:
          _tenantPropagationStrategy.propagate(refreshMessage, nodeType, nodeConfig);
          break;
        default:
          throw new IllegalArgumentException("Unsupported propagation type: " + propagationType);
      }
    }
  }
}
