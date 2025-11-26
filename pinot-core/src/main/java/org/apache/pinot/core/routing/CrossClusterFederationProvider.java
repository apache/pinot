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
import org.apache.pinot.common.utils.config.QueryOptionsUtils;


/**
 * A generic class which provides the dependencies for federation routing.
 * This class is responsible for managing routing managers and providing the appropriate
 * routing manager based on query options (e.g., whether federation is enabled).
 */
public class CrossClusterFederationProvider {
  // Maps clusterName to TableCache.
  private final Map<String, TableCache> _tableCacheMap;

  // Primary routing manager (for non-federated queries)
  private final RoutingManager _primaryRoutingManager;

  // Federated routing manager (for federated queries, may be null if federation is not configured)
  @Nullable
  private final RoutingManager _federatedRoutingManager;

  /**
   * Constructor for FederationProvider with routing managers.
   *
   * @param tableCacheMap Map of cluster name to TableCache
   * @param primaryRoutingManager Primary routing manager for non-federated queries
   * @param federatedRoutingManager Federated routing manager for federated queries (can be null)
   */
  public CrossClusterFederationProvider(Map<String, TableCache> tableCacheMap, RoutingManager primaryRoutingManager,
      @Nullable RoutingManager federatedRoutingManager) {
    _tableCacheMap = tableCacheMap;
    _primaryRoutingManager = primaryRoutingManager;
    _federatedRoutingManager = federatedRoutingManager;
  }

  public Map<String, TableCache> getTableCacheMap() {
    return _tableCacheMap;
  }

  public TableCache getTableCache(String clusterName) {
    return _tableCacheMap.get(clusterName);
  }

  /**
   * Returns the appropriate routing manager based on query options.
   * If federation is enabled in query options and a federated routing manager is available,
   * returns the federated routing manager. Otherwise, returns the primary routing manager.
   *
   * @param queryOptions Query options containing federation flag
   * @return The appropriate routing manager for the query
   */
  public RoutingManager getRoutingManager(Map<String, String> queryOptions) {
    boolean isFederationEnabled = QueryOptionsUtils.isEnableFederation(queryOptions, false);
    if (isFederationEnabled && _federatedRoutingManager != null) {
      return _federatedRoutingManager;
    }
    return _primaryRoutingManager;
  }

  /**
   * Returns the federated routing manager if available.
   *
   * @return Federated routing manager, or null if not configured
   */
  @Nullable
  public RoutingManager getFederatedRoutingManager() {
    return _federatedRoutingManager;
  }
}
