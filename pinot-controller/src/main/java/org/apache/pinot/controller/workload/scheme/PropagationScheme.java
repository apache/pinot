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
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.workload.NodeConfig;
import org.apache.pinot.spi.config.workload.PropagationEntity;
import org.apache.pinot.spi.config.workload.PropagationEntityOverrides;

/**
 * PropagationScheme is used to resolve instances based on the {@link NodeConfig}
 * 1. It helps to identify which instances to propagate the workload to based on the node configuration
 * 2. It helps among which instances the {@link org.apache.pinot.spi.config.workload.EnforcementProfile} should be split
 */
public interface PropagationScheme {
  /**
   * Resolves the set of instances to which the workload should be propagated based on the given entity and node type.
   *
   * @param entity The propagation entity containing scope and other relevant information.
   * @param nodeType The type of node (e.g., BROKER, SERVER) for which instances are to be resolved.
   * @param override Optional overrides that may affect instance resolution.
   * @return A set of instance names that match the criteria defined by the entity and node type.
   */
  Set<String> resolveInstances(PropagationEntity entity, NodeConfig.Type nodeType,
                               @Nullable PropagationEntityOverrides override);

  /**
   * Instance resolution can be different for overrides.
   * This method indicates if the current scheme supports overrides.
   * @return true if overrides are supported, false otherwise.
   */
  default boolean isOverrideSupported(PropagationEntity entity) {
    return false;
  }
}
