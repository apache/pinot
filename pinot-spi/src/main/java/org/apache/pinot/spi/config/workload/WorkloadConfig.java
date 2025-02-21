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


public class WorkloadConfig extends BaseJsonConfig {

  private static final String WORKLOAD_ID = "id";
  private static final String ENFORCEMENT_PROFILE = "enforcementProfile";

  @JsonPropertyDescription("Unique identifier for the workload")
  private String _workloadId;

  @JsonPropertyDescription("Enforcement profile for the workload")
  private EnforcementProfile _enforcementProfile;

  @JsonCreator
  public WorkloadConfig(@JsonProperty(value = WORKLOAD_ID, required = true) String workloadId,
      @JsonProperty(value = ENFORCEMENT_PROFILE, required = true) EnforcementProfile enforcementProfile) {
    _workloadId = workloadId;
    _enforcementProfile = enforcementProfile;
  }

  public String getWorkloadId() {
    return _workloadId;
  }

  public EnforcementProfile getEnforcementProfile() {
    return _enforcementProfile;
  }

  public void setWorkloadId(String workloadId) {
    _workloadId = workloadId;
  }

  public void setEnforcementProfile(EnforcementProfile enforcementProfile) {
    _enforcementProfile = enforcementProfile;
  }

}
