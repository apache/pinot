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


public class InstanceCost {

  private static final String CPU_COST_NS = "cpuCostNs";
  private static final String MEMORY_COST_BYTES = "memoryCostBytes";

  @JsonPropertyDescription("CPU cost of the instance in nanoseconds")
  private long _cpuCostNs;
  @JsonPropertyDescription("Memory cost of the instance in bytes")
  private long _memoryCostBytes;

  @JsonCreator
  public InstanceCost(@JsonProperty(CPU_COST_NS) long cpuCostNs,
                      @JsonProperty(MEMORY_COST_BYTES) long memoryCostBytes) {
    _cpuCostNs = cpuCostNs;
    _memoryCostBytes = memoryCostBytes;
  }

  public long getCpuCostNs() {
    return _cpuCostNs;
  }

  public long getMemoryCostBytes() {
    return _memoryCostBytes;
  }
}
