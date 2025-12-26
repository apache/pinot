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
package org.apache.pinot.core.routing;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;


/**
 * A generic class which provides the dependencies for federation routing.
 * This class is responsible for managing routing managers and providing the appropriate
 * routing manager based on query options (e.g., whether federation is enabled).
 */
public class MultiClusterRoutingContext {
  // Maps clusterName to TableCache. Includes the local and all remote clusters.
  private final Map<String, TableCache> _tableCacheMap;

  // Local
  private final RoutingManager _localRoutingManager;

  // Federated routing manager (for federated queries, may be null if federation is not configured)
  @Nullable
  private final RoutingManager _multiClusterRoutingManager;

  /**
   * Constructor for FederationProvider with routing managers.
   *
   * @param tableCacheMap Map of cluster name to TableCache
   * @param localRoutingManager Local routing manager for non-federated queries
   * @param multiClusterRoutingManager Multi cluster routing manager for cross-cluster queries (can be null)
   */
  public MultiClusterRoutingContext(Map<String, TableCache> tableCacheMap, RoutingManager localRoutingManager,
      @Nullable RoutingManager multiClusterRoutingManager) {
    _tableCacheMap = tableCacheMap;
    _localRoutingManager = localRoutingManager;
    _multiClusterRoutingManager = multiClusterRoutingManager;
  }
}
