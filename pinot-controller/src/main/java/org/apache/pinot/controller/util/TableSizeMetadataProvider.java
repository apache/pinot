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
package org.apache.pinot.controller.util;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.common.exception.InvalidConfigException;
import org.apache.pinot.common.restlet.resources.TableSizeInfo;
import org.apache.pinot.spi.auth.AuthProvider;


/// Supplies backend-independent inputs for table-size collection. Each read captures its table configuration and
/// desired replicas before HTTP collection; implementations must not substitute desired state for observed size.
/// Snapshots copy their collections. The auth provider remains a live credential source so rotation is honored.
/// Implementations must support concurrent reads, and recheck current ownership when publishing metrics.
public interface TableSizeMetadataProvider {
  Snapshot readTableSizes(String tableName, boolean includeReplacedSegments) throws InvalidConfigException;

  Replicas readReplicaAssignments(String tableNameWithType, boolean includeReplacedSegments)
      throws InvalidConfigException;

  boolean canPublishMetrics(String tableNameWithType);

  /// Applies backend-specific session/content identity checks before a server report contributes size. Invalid
  /// reports must be omitted, leaving their expected replicas missing; they must never become successful zeroes.
  /// The legacy server protocol has no native identity, so the Helix adapter preserves its existing behavior.
  default Map<String, TableSizeInfo> validateServerReports(Replicas replicas,
      Map<String, TableSizeInfo> serverReports) {
    return serverReports;
  }

  /// The configuration inputs for both table types, captured once for a table-size request.
  record Snapshot(@Nullable Table offline, @Nullable Table realtime) {
  }

  /// Immutable configuration projection; excludes mutable TableConfig objects from a captured read.
  record Table(String tableNameWithType, int replicaCount, boolean compressionStatsEnabled, Replicas replicas) {
  }

  /// Expected segment names and admin endpoints for the captured replicas. Expected replicas may have no endpoint;
  /// their reports remain missing. Authentication is resolved afresh by each HTTP request.
  record Replicas(Map<String, List<String>> serverToSegments, BiMap<String, String> serverEndpoints,
                  @Nullable AuthProvider authProvider) {
    public Replicas {
      Map<String, List<String>> segments = new HashMap<>();
      serverToSegments.forEach((server, names) -> segments.put(server, List.copyOf(names)));
      serverToSegments = Map.copyOf(segments);
      serverEndpoints = ImmutableBiMap.copyOf(serverEndpoints);
    }
  }
}
