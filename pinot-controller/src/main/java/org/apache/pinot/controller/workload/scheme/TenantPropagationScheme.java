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
package org.apache.pinot.controller.workload.scheme;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.utils.config.TagNameUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.workload.splitter.CostSplitter;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;


/**
 * TenantPropagationScheme is used to resolve instances based on the {@link NodeConfig} and {@link NodeConfig.Type}
 * It resolves the instances based on the tenants specified in the node configuration
 */
public class TenantPropagationScheme implements PropagationScheme {

  private final PinotHelixResourceManager _pinotHelixResourceManager;

  public TenantPropagationScheme(PinotHelixResourceManager pinotHelixResourceManager) {
    _pinotHelixResourceManager = pinotHelixResourceManager;
  }

  @Override
  public Set<String> resolveInstances(NodeConfig nodeConfig) {
    Set<String> instances = new HashSet<>();
    Map<String, Set<String>> helixTagToInstances = PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);
    NodeConfig.Type nodeType = nodeConfig.getNodeType();
    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      Set<String> resolvedInstances = resolveInstances(entity, nodeType, helixTagToInstances);
      if (!resolvedInstances.isEmpty()) {
        instances.addAll(resolvedInstances);
      } else {
        throw new IllegalArgumentException("No instances found for PropagationEntity: "
            + entity.getEntity());
      }
    }
    return instances;
  }

  @Override
  public Map<String, InstanceCost> resolveInstanceCostMap(NodeConfig nodeConfig, CostSplitter costSplitter) {
    Map<String, InstanceCost> instanceCostMap = new HashMap<>();
    Map<String, Set<String>> helixTagToInstances = PropagationUtils.getHelixTagToInstances(_pinotHelixResourceManager);
    for (PropagationEntity entity : nodeConfig.getPropagationScheme().getPropagationEntities()) {
      if (entity.getOverrides() != null) {
        throw new IllegalArgumentException("Sub-allocations are not supported in TenantPropagationScheme");
      }
      Set<String> instances = resolveInstances(entity, nodeConfig.getNodeType(), helixTagToInstances);
      if (instances.isEmpty()) {
        // This is to ensure cost splits are added for active tenants
        throw new IllegalArgumentException("No instances found for PropagationEntity: " + entity);
      }
      Map<String, InstanceCost> delta = costSplitter.computeInstanceCostMap(entity.getCpuCostNs(),
          entity.getMemoryCostBytes(), instances);
      PropagationUtils.mergeCosts(instanceCostMap, delta);
    }
    return instanceCostMap;
  }

  public Set<String> resolveInstances(PropagationEntity entity, NodeConfig.Type nodeType,
                                       Map<String, Set<String>> helixTagToInstances) {
    String tenantName = entity.getEntity();
    Set<String> allInstances = new HashSet<>();
    // Get the unique set of helix tags for the tenants
    Set<String> helixTags = new HashSet<>();
    if (nodeType == NodeConfig.Type.BROKER_NODE) {
      helixTags.add(TagNameUtils.getBrokerTagForTenant(tenantName));
    } else if (nodeType == NodeConfig.Type.SERVER_NODE) {
      if (TagNameUtils.isOfflineServerTag(tenantName) || TagNameUtils.isRealtimeServerTag(tenantName)) {
        helixTags.add(tenantName);
      } else {
        helixTags.add(TagNameUtils.getOfflineTagForTenant(tenantName));
        helixTags.add(TagNameUtils.getRealtimeTagForTenant(tenantName));
      }
    }
    // Get the instances for the helix tags
    for (String helixTag : helixTags) {
      Set<String> instances = helixTagToInstances.get(helixTag);
      if (instances != null) {
        allInstances.addAll(instances);
      }
    }
    return allInstances;
  }
}
