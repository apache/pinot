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
package org.apache.pinot.controller.helix.core.version;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;


/**
 * Cluster-wide snapshot of version information, grouped by component type.
 *
 * <p>The snapshot reflects the state of Helix InstanceConfig at the moment the controller
 * last refreshed its internal cache. The {@link #getSnapshotTimeMs()} field indicates when
 * the snapshot was taken; a stale value indicates the cache has not yet been refreshed.
 *
 * <p>{@link #isDataAvailable()} will be {@code false} when the last Helix read failed and
 * no cached data is available.
 */
public class ClusterVersionSummary {

  @JsonProperty("clusterName")
  private final String _clusterName;

  @JsonProperty("snapshotTimeMs")
  private final long _snapshotTimeMs;

  /** {@code false} when Helix read failed and no cached data exists. */
  @JsonProperty("dataAvailable")
  private final boolean _dataAvailable;

  /** Keyed by component type string: {@code CONTROLLER}, {@code BROKER}, {@code SERVER}, {@code MINION}. */
  @JsonProperty("componentSummaries")
  private final Map<String, ComponentVersionSummary> _componentSummaries;

  public ClusterVersionSummary(String clusterName, long snapshotTimeMs, boolean dataAvailable,
      Map<String, ComponentVersionSummary> componentSummaries) {
    _clusterName = clusterName;
    _snapshotTimeMs = snapshotTimeMs;
    _dataAvailable = dataAvailable;
    _componentSummaries = componentSummaries;
  }

  public String getClusterName() {
    return _clusterName;
  }

  public long getSnapshotTimeMs() {
    return _snapshotTimeMs;
  }

  public boolean isDataAvailable() {
    return _dataAvailable;
  }

  public Map<String, ComponentVersionSummary> getComponentSummaries() {
    return _componentSummaries;
  }
}
