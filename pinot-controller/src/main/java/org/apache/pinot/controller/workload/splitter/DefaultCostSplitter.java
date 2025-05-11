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
package org.apache.pinot.controller.workload.splitter;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;


public class DefaultCostSplitter implements CostSplitter {

  @Override
  public Map<String, InstanceCost> computeInstanceCostMap(NodeConfig nodeConfig, Set<String> instances) {
    InstanceCost cost = computeInstanceCost(nodeConfig, instances, null);
    Map<String, InstanceCost> costMap = new HashMap<>();
    for (String instance : instances) {
      costMap.put(instance, cost);
    }
    return costMap;
  }

  @Override
  public InstanceCost computeInstanceCost(NodeConfig nodeConfig, Set<String> instances, String instance) {
    long totalInstances = instances.size();
    EnforcementProfile enforcementProfile = nodeConfig.getEnforcementProfile();

    long cpuCostNs = enforcementProfile.getCpuCostNs() / totalInstances;
    long memoryCostBytes = enforcementProfile.getMemoryCostBytes() / totalInstances;

    return new InstanceCost(cpuCostNs, memoryCostBytes);
  }
}
