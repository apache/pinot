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


@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkloadCost extends BaseJsonConfig {

  private static final String CPU_COST = "cpuCost";
  private static final String MEMORY_COST = "memoryCost";

  @JsonPropertyDescription("Max CPU cost allowed for the workload")
  private double _cpuCost;

  @JsonPropertyDescription("Max memory cost allowed for the workload")
  private double _memoryCost;

  public WorkloadCost(@JsonProperty(CPU_COST) double cpuCost, @JsonProperty(MEMORY_COST) double memoryCost) {
    _cpuCost = cpuCost;
    _memoryCost = memoryCost;
  }

  public double getCpuCost() {
    return _cpuCost;
  }

  public double getMemoryCost() {
    return _memoryCost;
  }

  public void setCpuCost(double cpuCost) {
    _cpuCost = cpuCost;
  }

  public void setMemoryCost(double memoryCost) {
    _memoryCost = memoryCost;
  }
}
