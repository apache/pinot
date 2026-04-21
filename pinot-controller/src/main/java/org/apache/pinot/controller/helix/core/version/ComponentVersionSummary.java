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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * Aggregated version information for all live instances of one component type.
 *
 * <p>Only <em>live</em> instances (those with an ephemeral Helix LiveInstance ZNode) are
 * counted in {@link #getVersionCounts()}, {@link #getMinVersion()}, and
 * {@link #getMaxVersion()}.  Dead / offline instances are tracked separately in
 * {@link #getOfflineInstanceCount()} for observability but excluded from compatibility
 * calculations to prevent stale InstanceConfigs from skewing the minimum version.
 */
public class ComponentVersionSummary {

  @JsonProperty("componentType")
  private final String _componentType;

  @JsonProperty("liveInstanceCount")
  private final int _liveInstanceCount;

  @JsonProperty("offlineInstanceCount")
  private final int _offlineInstanceCount;

  /** Map of rawVersion → number of live instances reporting that version. */
  @JsonProperty("versionCounts")
  private final Map<String, Integer> _versionCounts;

  /** Minimum version among live instances with a known version. Null if none are known. */
  @JsonProperty("minVersion")
  @Nullable
  private final String _minVersion;

  /** Maximum version among live instances with a known version. Null if none are known. */
  @JsonProperty("maxVersion")
  @Nullable
  private final String _maxVersion;

  /** Count of live instances reporting {@code UNKNOWN} or an unparseable version. */
  @JsonProperty("unknownVersionCount")
  private final int _unknownVersionCount;

  @JsonProperty("instances")
  private final List<InstanceVersionInfo> _instances;

  public ComponentVersionSummary(String componentType, int liveInstanceCount,
      int offlineInstanceCount, Map<String, Integer> versionCounts,
      @Nullable String minVersion, @Nullable String maxVersion,
      int unknownVersionCount, List<InstanceVersionInfo> instances) {
    _componentType = componentType;
    _liveInstanceCount = liveInstanceCount;
    _offlineInstanceCount = offlineInstanceCount;
    _versionCounts = versionCounts == null
        ? Collections.emptyMap() : Collections.unmodifiableMap(versionCounts);
    _minVersion = minVersion;
    _maxVersion = maxVersion;
    _unknownVersionCount = unknownVersionCount;
    _instances = instances == null
        ? Collections.emptyList() : Collections.unmodifiableList(instances);
  }

  public String getComponentType() {
    return _componentType;
  }

  public int getLiveInstanceCount() {
    return _liveInstanceCount;
  }

  public int getOfflineInstanceCount() {
    return _offlineInstanceCount;
  }

  public Map<String, Integer> getVersionCounts() {
    return _versionCounts;
  }

  @Nullable
  public String getMinVersion() {
    return _minVersion;
  }

  @Nullable
  public String getMaxVersion() {
    return _maxVersion;
  }

  public int getUnknownVersionCount() {
    return _unknownVersionCount;
  }

  public List<InstanceVersionInfo> getInstances() {
    return _instances;
  }
}
