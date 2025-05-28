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

/**
 * Represents the resource cost profile of an instance for query workload enforcement.
 * <p>
 * This class defines the CPU and memory cost thresholds that an instance can incur
 * when executing queries, used when propagating workload configurations to individual instances.
 * </p>
 *
 */
public class InstanceCost {

  private static final String CPU_COST_NS = "cpuCostNs";
  private static final String MEMORY_COST_BYTES = "memoryCostBytes";

  /**
   * The CPU cost threshold for the instance, in nanoseconds.
   */
  @JsonPropertyDescription("CPU cost of the instance in nanoseconds")
  private long _cpuCostNs;
  /**
   * The memory cost threshold for the instance, in bytes.
   */
  @JsonPropertyDescription("Memory cost of the instance in bytes")
  private long _memoryCostBytes;

  /**
   * Constructs an InstanceCost profile with specified CPU and memory thresholds.
   *
   * @param cpuCostNs     CPU cost threshold in nanoseconds for this instance
   * @param memoryCostBytes memory cost threshold in bytes for this instance
   */
  @JsonCreator
  public InstanceCost(@JsonProperty(CPU_COST_NS) long cpuCostNs,
                      @JsonProperty(MEMORY_COST_BYTES) long memoryCostBytes) {
    _cpuCostNs = cpuCostNs;
    _memoryCostBytes = memoryCostBytes;
  }

  /**
   * Returns the CPU cost threshold for this instance.
   *
   * @return CPU cost limit in nanoseconds
   */
  public long getCpuCostNs() {
    return _cpuCostNs;
  }

  /**
   * Returns the memory cost threshold for this instance.
   *
   * @return memory cost limit in bytes
   */
  public long getMemoryCostBytes() {
    return _memoryCostBytes;
  }
}
