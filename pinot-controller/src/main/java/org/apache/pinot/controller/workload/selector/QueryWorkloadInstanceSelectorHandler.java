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
package org.apache.pinot.controller.workload.selector;

import java.util.Set;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationScheme;


/**
 * The PropagationManager class is responsible for propagating the query workload refresh message to the relevant
 * instances based on the node configurations.
 */
public class QueryWorkloadInstanceSelectorHandler {

  private final TableInstanceSelector _tableInstanceSelector;
  private final TenantInstanceSelector _tenantInstanceSelector;
  private final DefaultInstanceSelector _defaultInstanceSelector;

  public QueryWorkloadInstanceSelectorHandler(PinotHelixResourceManager pinotHelixResourceManager) {
    _tableInstanceSelector = new TableInstanceSelector(pinotHelixResourceManager);
    _tenantInstanceSelector = new TenantInstanceSelector(pinotHelixResourceManager);
    _defaultInstanceSelector = new DefaultInstanceSelector(pinotHelixResourceManager);
  }

  public Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances;
    PropagationScheme.Type propagationType = nodeConfig.getPropagationScheme().getPropagationType();
    switch (propagationType) {
      case TABLE:
        instances = _tableInstanceSelector.resolveInstances(nodeType, nodeConfig);
        break;
      case TENANT:
        instances = _tenantInstanceSelector.resolveInstances(nodeType, nodeConfig);
        break;
      default:
        instances = _defaultInstanceSelector.resolveInstances(nodeType, nodeConfig);
        break;
    }
    return instances;
  }
}
