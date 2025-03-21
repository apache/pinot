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

  private static final String CPU_COST = "cpuCost";
  private static final String MEMORY_COST = "memoryCost";
  private static final String ENFORCEMENT_PERIOD_MILLIS = "enforcementPeriodMillis";

  @JsonPropertyDescription("CPU cost of the instance")
  private long _cpuCost;
  @JsonPropertyDescription("Memory cost of the instance")
  private long _memoryCost;
  @JsonPropertyDescription("Enforcement period in milliseconds")
  private long _enforcementPeriodMillis;

  @JsonCreator
  public InstanceCost(@JsonProperty(CPU_COST) long cpuCost, @JsonProperty(MEMORY_COST) long memoryCost,
      @JsonProperty(ENFORCEMENT_PERIOD_MILLIS) long enforcementPeriodMillis) {
    _cpuCost = cpuCost;
    _memoryCost = memoryCost;
    _enforcementPeriodMillis = enforcementPeriodMillis;
  }

  public long getCpuCost() {
    return _cpuCost;
  }

  public long getMemoryCost() {
    return _memoryCost;
  }

  public long getEnforcementPeriodMillis() {
    return _enforcementPeriodMillis;
  }

  public void setCpuCost(long cpuCost) {
    _cpuCost = cpuCost;
  }

  public void setMemoryCost(long memoryCost) {
    _memoryCost = memoryCost;
  }

  public void setEnforcementPeriodMillis(long enforcementPeriodMillis) {
    _enforcementPeriodMillis = enforcementPeriodMillis;
  }
}