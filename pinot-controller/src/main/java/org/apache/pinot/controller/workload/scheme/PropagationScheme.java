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

import java.util.Set;
import org.apache.pinot.spi.config.workload.NodeConfig;

/**
 * PropagationScheme is used to resolve instances based on the {@link NodeConfig} and {@link NodeConfig.Type}
 * 1. It helps to identify which instances to propagate the workload to based on the node configuration
 * 2. It helps among which instances the {@link org.apache.pinot.spi.config.workload.EnforcementProfile} should be split
 */
public interface PropagationScheme {
  /**
   * Resolve the instances based on the node type and node configuration
   * @param nodeType {@link NodeConfig.Type}
   * @param nodeConfig The {@link NodeConfig} to resolve the instances
   * @return The set of instances to propagate the workload to
   */
  Set<String> resolveInstances(NodeConfig.Type nodeType, NodeConfig nodeConfig);
}
