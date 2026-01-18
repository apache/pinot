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
package org.apache.pinot.spi.config.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.spi.config.BaseJsonConfig;

/**
 * Represents a propagation entity with resource allocation constraints.
 * <p>
 * A PropagationEntity defines resource limits (CPU and memory) for a specific entity
 * (such as a table or tenant) within a workload propagation scheme. It can also contain
 * nested sub-allocations for more granular resource management.
 * </p>
 */
public class PropagationEntity extends BaseJsonConfig {
  @JsonPropertyDescription("Describes the unique identifier for the propagation entity (table, tenant, etc.)")
  private String _entity;

  @JsonPropertyDescription("Max CPU cost allowed for the propagation entity, if not specified, inherited from parent")
  private Long _cpuCostNs;

  @JsonPropertyDescription("Max memory cost allowed for the propagation entity, "
      + "if not specified, inherited from parent")
  private Long _memoryCostBytes;

  /**
   * List of finer grained resource allocation within the entity.
   * For example, in a TablePropagationScheme, for realtime tables, the overrides could be "CONSUMING" and "COMPLETED"
   * to allow for two different resource allocations. Each having its own CPU and memory costs.
   */
  @JsonPropertyDescription("List of overrides for finer grained resource allocation within the entity")
  private List<PropagationEntityOverrides> _overrides;

  @JsonCreator
  public PropagationEntity(@JsonProperty("entity") String entity,
                           @Nullable @JsonProperty("cpuCostNs") Long cpuCostNs,
                           @Nullable @JsonProperty("memoryCostBytes") Long memoryCostBytes,
                           @Nullable @JsonProperty("overrides") List<PropagationEntityOverrides> overrides) {
    _entity = entity;
    _cpuCostNs = cpuCostNs;
    _memoryCostBytes = memoryCostBytes;
    _overrides = overrides;
  }

  public String getEntity() {
    return _entity;
  }

  @Nullable
  public Long getCpuCostNs() {
    return _cpuCostNs;
  }

  @Nullable
  public Long getMemoryCostBytes() {
    return _memoryCostBytes;
  }

  @Nullable
  public List<PropagationEntityOverrides> getOverrides() {
    return _overrides;
  }

  public void setEntity(String entity) {
    _entity = entity;
  }

  public void setCpuCostNs(long cpuCostNs) {
    _cpuCostNs = cpuCostNs;
  }

  public void setMemoryCostBytes(long memoryCostBytes) {
    _memoryCostBytes = memoryCostBytes;
  }

  public void setOverrides(List<PropagationEntityOverrides> overrides) {
    _overrides = overrides;
  }
}
