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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.spi.config.workload.NodeConfig;


/**
 * The TenantPropagationInstanceSelector is responsible for selecting instances hosting the tenant's tables
 * defined in the {@link org.apache.pinot.spi.config.workload.PropagationScheme}.
 * For non-leaf nodes, select all the broker instances that serve the all the tables in tenant specified.
 * For leaf nodes, select all the server instances that serve all the all tables in the tenant specified.
 */
public class TenantInstanceSelector implements InstanceSelector {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TenantInstanceSelector(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    List<String> tenants = nodeConfig.getPropagationScheme().getValues();
    switch (nodeType) {
      case NON_LEAF_NODE:
        for (String tenant : tenants) {
          instances.addAll(_pinotHelixResourceManager.getAllInstancesForBrokerTenant(tenant));
        }
        break;
      case LEAF_NODE:
        for (String tenant : tenants) {
          instances.addAll(_pinotHelixResourceManager.getAllInstancesForServerTenant(tenant));
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported node type: " + nodeType);
    }
    return instances;
  }
}
