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

import java.util.Map;
import org.apache.pinot.spi.config.workload.InstanceCost;
import org.apache.pinot.spi.config.workload.NodeConfig;

/**
 * Interface for splitting the cost of a workload between instances.
 */
public interface CostSplitter {
  /**
   * Computes the cost for each instance in the given set of instances.
   *
   * @param nodeConfig the node configuration
   * @param instancesInfo info about all instances involved
   * @return a map from instance identifier to the cost for that instance
   */
  Map<String, InstanceCost> getInstanceCostMap(NodeConfig nodeConfig, InstancesInfo instancesInfo);

  /**
   * Computes the cost for a specific instance.
   *
   * @param nodeConfig the node configuration
   * @param instancesInfo info about all instances involved
   * @param instance the instance identifier for which to compute the cost
   * @return the cost for the specified instance
   */
  InstanceCost getInstanceCost(NodeConfig nodeConfig, InstancesInfo instancesInfo, String instance);
}
