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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.utils.config.QueryOptionsUtils;
import org.apache.pinot.spi.exception.QueryErrorCode;


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

  // Set of cluster names that failed to connect (for warning in query responses)
  private final Set<String> _unavailableClusters;

  /**
   * Constructor for FederationProvider with routing managers and unavailable clusters.
   *
   * @param tableCacheMap Map of cluster name to TableCache
   * @param localRoutingManager Local routing manager for non-federated queries
   * @param multiClusterRoutingManager Multi cluster routing manager for cross-cluster queries (can be null)
   */
  public MultiClusterRoutingContext(Map<String, TableCache> tableCacheMap, RoutingManager localRoutingManager,
      @Nullable RoutingManager multiClusterRoutingManager, Set<String> unavailableClusters) {
    _tableCacheMap = tableCacheMap;
    _localRoutingManager = localRoutingManager;
    _multiClusterRoutingManager = multiClusterRoutingManager;
    _unavailableClusters = unavailableClusters != null ? unavailableClusters : Collections.emptySet();
  }

  public Map<String, TableCache> getTableCacheMap() {
    return _tableCacheMap;
  }

  public TableCache getTableCache(String clusterName) {
    return _tableCacheMap.get(clusterName);
  }

  /**
   * Returns the appropriate routing manager based on query options.
   * If federation is enabled in query options and a multi cluster routing manager is available,
   * returns the federated routing manager. Otherwise, returns the primary routing manager.
   *
   * @param queryOptions Query options containing federation flag
   * @return The appropriate routing manager for the query
   */
  public RoutingManager getRoutingManager(Map<String, String> queryOptions) {
    boolean isMultiClusterRoutingEnabled = QueryOptionsUtils.isMultiClusterRoutingEnabled(queryOptions, false);
    if (isMultiClusterRoutingEnabled && _multiClusterRoutingManager != null) {
      return _multiClusterRoutingManager;
    }
    return _localRoutingManager;
  }

  public RoutingManager getMultiClusterRoutingManager() {
    return _multiClusterRoutingManager;
  }

  public boolean hasUnavailableClusters() {
    return !_unavailableClusters.isEmpty();
  }

  public List<QueryProcessingException> getUnavailableClusterExceptions() {
    if (_unavailableClusters.isEmpty()) {
      return Collections.emptyList();
    }
    List<QueryProcessingException> exceptions = new ArrayList<>();
    for (String clusterName : _unavailableClusters) {
      String message = String.format("Remote cluster '%s' is not connected. "
          + "Query results may be incomplete.", clusterName);
      exceptions.add(new QueryProcessingException(QueryErrorCode.REMOTE_CLUSTER_UNAVAILABLE, message));
    }
    return exceptions;
  }
}
