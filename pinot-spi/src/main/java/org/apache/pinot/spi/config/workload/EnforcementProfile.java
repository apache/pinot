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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.pinot.spi.config.BaseJsonConfig;

/**
 * Defines the resource enforcement profile for a node within a query workload.
 * <p>
 * This profile specifies the maximum CPU time (in nanoseconds) and maximum memory (in bytes)
 * that queries under this workload are allowed to consume on the node.
 * </p>
 *
 * @see QueryWorkloadConfig
 * @see NodeConfig
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class EnforcementProfile extends BaseJsonConfig {

  private static final String CPU_COST_NS = "cpuCostNs";
  private static final String MEMORY_COST_BYTES = "memoryCostBytes";

  @JsonPropertyDescription("Max CPU cost allowed for the workload")
  private long _cpuCostNs;
  @JsonPropertyDescription("Max memory cost allowed for the workload")
  private long _memoryCostBytes;

  /**
   * Constructs an EnforcementProfile with specified resource limits.
   *
   * @param cpuCostNs maximum allowed CPU cost in nanoseconds for the workload
   * @param memoryCostBytes maximum allowed memory cost in bytes for the workload
   */
  public EnforcementProfile(@JsonProperty(CPU_COST_NS) long cpuCostNs,
                            @JsonProperty(MEMORY_COST_BYTES) long memoryCostBytes) {
    _cpuCostNs = cpuCostNs;
    _memoryCostBytes = memoryCostBytes;
  }

  /**
   * Returns the maximum CPU cost allowed for this workload.
   *
   * @return CPU cost limit in nanoseconds
   */
  public long getCpuCostNs() {
    return _cpuCostNs;
  }

  /**
   * Returns the maximum memory cost allowed for this workload.
   *
   * @return memory cost limit in bytes
   */
  public long getMemoryCostBytes() {
    return _memoryCostBytes;
  }
}
