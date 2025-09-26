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
import org.apache.pinot.spi.config.BaseJsonConfig;

/**
 * Represents custom entity overrides for propagation schemes.
 *
 * It defines custom entity overrides where finer grained resource allocations is needed.
 * For example, in a TablePropagationScheme, for realtime tables. The entity could be "CONSUMING" or "COMPLETED"
 * to allow for two different resource allocations splits. Each having its own CPU and memory costs.
 */
public class PropagationEntityOverrides extends BaseJsonConfig {

  @JsonPropertyDescription("The unique identifier for the propagation entity override")
  private String _entity;

  @JsonPropertyDescription("The CPU cost in nanoseconds")
  private Long _cpuCostNs;

  @JsonPropertyDescription("The memory cost in bytes")
  private Long _memoryCostBytes;

  @JsonCreator
  public PropagationEntityOverrides(@JsonProperty("entity") String entity,
                                    @JsonProperty("cpuCostNs") Long cpuCostNs,
                                    @JsonProperty("memoryCostBytes") Long memoryCostBytes) {
    _entity = entity;
    _cpuCostNs = cpuCostNs;
    _memoryCostBytes = memoryCostBytes;
  }

  public String getEntity() {
    return _entity;
  }

  public Long getCpuCostNs() {
    return _cpuCostNs;
  }

  public Long getMemoryCostBytes() {
    return _memoryCostBytes;
  }

  public void setEntity(String entity) {
    _entity = entity;
  }

  public void setCpuCostNs(Long cpuCostNs) {
    _cpuCostNs = cpuCostNs;
  }

  public void setMemoryCostBytes(Long memoryCostBytes) {
    _memoryCostBytes = memoryCostBytes;
  }
}
