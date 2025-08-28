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

public class CostSplit extends BaseJsonConfig {
  @JsonPropertyDescription("Describes the unique identifier for cost allocation could be a table,tenant etc.")
  private String _costId;

  @JsonPropertyDescription("Max CPU cost allowed for the cost id, if not specified, inherited from parent")
  private Long _cpuCostNs;

  @JsonPropertyDescription("Max memory cost allowed for the cost id, if not specified, inherited from parent")
  private Long _memoryCostBytes;

  /**
   * Optional nested allocations. Omitted or empty when not used.
   * Could be used to represent sub-allocations within the cost id,
   * such as allocations for CONSUMING/COMPLETED tenants within a realtime table.
   */
  @JsonPropertyDescription("Optional nested allocations for the cost id")
  private List<CostSplit> _subAllocations;

  @JsonCreator
  public CostSplit(@JsonProperty("costId") String costId,
                   @Nullable @JsonProperty("cpuCostNs") Long cpuCostNs,
                   @Nullable @JsonProperty("memoryCostBytes") Long memoryCostBytes,
                   @Nullable @JsonProperty("subAllocations") List<CostSplit> subAllocations) {
    _costId = costId;
    _cpuCostNs = cpuCostNs;
    _memoryCostBytes = memoryCostBytes;
    _subAllocations = subAllocations;
  }

  public String getCostId() {
    return _costId;
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
  public List<CostSplit> getSubAllocations() {
    return _subAllocations;
  }

  public void setCostId(String costId) {
    _costId = costId;
  }

  public void setCpuCostNs(long cpuCostNs) {
    _cpuCostNs = cpuCostNs;
  }

  public void setMemoryCostBytes(long memoryCostBytes) {
    _memoryCostBytes = memoryCostBytes;
  }

  public void setSubAllocations(@Nullable List<CostSplit> subAllocations) {
    _subAllocations = subAllocations;
  }
}
