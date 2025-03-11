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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.common.messages.QueryWorkloadRefreshMessage;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;


/**
 * Propagation strategy for tenant based workload propagation.
 * This strategy propagates the workload refresh message to the instances that serve the given tenants.
 * For non-leaf nodes, the message is propagated to the broker instances that serve the given tenants.
 * For leaf nodes, the message is propagated to the server instances that serve the given tenants.
 */
public class TenantPropagationStrategy implements PropagationStrategy {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TenantPropagationStrategy(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public void propagate(QueryWorkloadRefreshMessage refreshMessage, NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    List<String> tables = nodeConfig.getPropagationScheme().getValues();
    if (nodeType == NodeConfig.Type.NON_LEAF_NODE) {
      for (String table : tables) {
        instances.addAll(_pinotHelixResourceManager.getAllInstancesForBrokerTenant(table));
      }
    } else if (nodeType == NodeConfig.Type.LEAF_NODE) {
      for (String table : tables) {
        instances.addAll(_pinotHelixResourceManager.getAllInstancesForServerTenant(table));
      }
    }
    _pinotHelixResourceManager.sendQueryWorkloadRefreshMessage(refreshMessage, instances);
  }
}
